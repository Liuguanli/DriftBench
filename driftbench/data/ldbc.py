"""Stage LDBC SNB Interactive v1 inputs and native driver handoff files.

Supports Hadoop Datagen 1.0.0 csv_merge_foreign and the SNB driver 1.2.0
configuration contract. Does not synthesize graphs, install tools, execute a
database, or establish LDBC conformance or graph referential integrity.
"""
from __future__ import annotations

import csv
import math
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import ClassVar

from .base import BenchmarkArtifact, GenerationResult
from ._input_artifacts import checked_output, signature, source_directory, source_file

_SUITE = "snb-interactive-v1"
_DATAGEN_REVISION = "37d35f40f5023fcf1afd3b6d0984f71c202f4bca"
_DRIVER_REVISION = "4cd13735f964406ad34f34ccd5bef4d6e6c284d0"
_PREFIX = "ldbc.snb.interactive."
_WORKLOAD = "org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkload"
_UPDATES = ["1AddPerson", "2AddPostLike", "3AddCommentLike", "4AddForum", "5AddForumMembership", "6AddPost", "7AddComment", "8AddFriendship"]
_SHORTS = ["1PersonProfile", "2PersonPosts", "3PersonFriends", "4MessageContent", "5MessageCreator", "6MessageForum", "7MessageReplies"]
_READ_KEYS = [f"{_PREFIX}LdbcQuery{i}_enable" for i in range(1, 15)]
_SHORT_KEYS = [f"{_PREFIX}LdbcShortQuery{name}_enable" for name in _SHORTS]
_UPDATE_KEYS = [f"{_PREFIX}LdbcUpdate{name}_enable" for name in _UPDATES]

# Positional headers from the pinned csv_merge_foreign serializers.
_HEADERS = {
    "static/organisation": "id|type|name|url|place",
    "static/place": "id|name|url|type|isPartOf",
    "static/tag": "id|name|url|hasType",
    "static/tagclass": "id|name|url|isSubclassOf",
    "dynamic/person": "id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|place",
    "dynamic/person_speaks_language": "Person.id|language",
    "dynamic/person_email_emailaddress": "Person.id|email",
    "dynamic/person_hasInterest_tag": "Person.id|Tag.id",
    "dynamic/person_workAt_organisation": "Person.id|Organisation.id|workFrom",
    "dynamic/person_studyAt_organisation": "Person.id|Organisation.id|classYear",
    "dynamic/person_knows_person": "Person.id|Person.id|creationDate",
    "dynamic/forum": "id|title|creationDate|moderator",
    "dynamic/forum_hasMember_person": "Forum.id|Person.id|joinDate",
    "dynamic/forum_hasTag_tag": "Forum.id|Tag.id",
    "dynamic/person_likes_post": "Person.id|Post.id|creationDate",
    "dynamic/person_likes_comment": "Person.id|Comment.id|creationDate",
    "dynamic/post": "id|imageFile|creationDate|locationIP|browserUsed|language|content|length|creator|Forum.id|place",
    "dynamic/post_hasTag_tag": "Post.id|Tag.id",
    "dynamic/comment": "id|creationDate|locationIP|browserUsed|content|length|creator|place|replyOfPost|replyOfComment",
    "dynamic/comment_hasTag_tag": "Comment.id|Tag.id",
}
_PARAMETERS = {
    1: "personId|firstName", 2: "personId|maxDate",
    3: "personId|startDate|durationDays|countryXName|countryYName",
    4: "personId|startDate|durationDays", 5: "personId|minDate", 6: "personId|tagName",
    7: "personId", 8: "personId", 9: "personId|maxDate", 10: "personId|month",
    11: "personId|countryName|workFromYear", 12: "personId|tagClassName",
    13: "person1Id|person2Id", 14: "person1Id|person2Id",
}


def _force(value: bool) -> None:
    if type(value) is not bool:
        raise ValueError("force must be a boolean")


def _rows(path: Path, header: str, *, dates: str | None = None, parameters: bool = False) -> int:
    columns = header.split("|")
    count = 0
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream, delimiter="|", quoting=csv.QUOTE_NONE)
        if next(reader, None) != columns:
            raise ValueError(f"{path.name}: header does not match the supported SNB v1 format")
        for index, values in enumerate(reader, 2):
            if len(values) != len(columns):
                raise ValueError(f"{path.name} line {index}: expected {len(columns)} fields")
            for column, value in zip(columns, values):
                try:
                    if parameters and column in {"personId", "person1Id", "person2Id", "maxDate", "startDate", "minDate", "durationDays", "month", "workFromYear"}:
                        number = int(value)
                        if not -(2**63) <= number < 2**63:
                            raise ValueError()
                        if column == "month" and not 1 <= number <= 12:
                            raise ValueError()
                        if column == "durationDays" and not 1 <= number < 2**31:
                            raise ValueError()
                    elif dates and column in {"creationDate", "birthday", "joinDate"}:
                        if dates == "epoch_millis":
                            int(value)
                        else:
                            # Datagen StringDateFormatter emits +HHMM; Python
                            # 3.10 requires +HH:MM. Only normalize for parsing.
                            normalized = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", value)
                            datetime.fromisoformat(normalized.replace("Z", "+00:00"))
                    elif parameters and not value:
                        raise ValueError()
                except ValueError as exc:
                    raise ValueError(f"{path.name} line {index}: invalid {column} for the declared format") from exc
            count += 1
    if parameters and count == 0:
        raise ValueError(f"{path.name}: at least one parameter row is required")
    return count


def _unescape_property(value: str) -> str:
    def decode(match):
        escaped = match.group(1)
        if escaped.startswith("u"):
            if len(escaped) != 5:
                raise ValueError("driver_config contains an invalid Java-properties escape")
            return chr(int(escaped[1:], 16))
        return {"t": "\t", "r": "\r", "n": "\n", "f": "\f"}.get(escaped, escaped)
    return re.sub(r"\\(u[0-9a-fA-F]{4}|.)", decode, value)


def _properties(path: Path) -> dict[str, str]:
    if path.stat().st_size > 65536:
        raise ValueError("driver_config must be a native .properties file of at most 64 KiB")
    properties: dict[str, str] = {}
    pending = ""
    for raw in path.read_bytes().decode("iso-8859-1").splitlines():
        trimmed = raw.lstrip(" \t\f")
        if not pending and (not trimmed or trimmed.startswith(("#", "!"))):
            continue
        line = pending + trimmed
        if (len(line) - len(line.rstrip("\\"))) % 2:
            pending = line[:-1]
            continue
        pending = ""
        if not line:
            continue
        match = re.match(r"((?:\\.|[^\s:=])*)(?:\s*[:=]\s*|\s+)?(.*)$", line)
        if match is None:
            raise ValueError("driver_config contains an invalid Java-properties entry")
        properties[_unescape_property(match[1])] = _unescape_property(match[2])
    if pending:
        raise ValueError("driver_config ends with an incomplete continuation")
    return properties


def _validate_config(config: dict[str, str], read_only: bool) -> None:
    if not re.fullmatch(r"[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)+", config.get("db", "")):
        raise ValueError("driver_config must provide the native 'db' implementation class for your target database")
    if config.get("mode") != "execute_benchmark":
        raise ValueError("driver_config must set mode=execute_benchmark")
    for name, maximum in (("operation_count", 2**63 - 1), ("thread_count", 2**31 - 1)):
        try:
            if not 1 <= int(config.get(name, "")) <= maximum:
                raise ValueError()
        except ValueError as exc:
            raise ValueError(f"driver_config must provide integer {name} in 1..{maximum}") from exc
    for name in ("time_compression_ratio",):
        try:
            value = float(config.get(name, ""))
            if not math.isfinite(value) or value <= 0:
                raise ValueError()
        except ValueError as exc:
            raise ValueError(f"driver_config must provide a positive finite {name}") from exc
    if config.get(_PREFIX + "scale_factor") not in {"0.1", "0.3", "1", "3", "10", "30", "100", "300", "1000"}:
        raise ValueError("driver_config must provide a supported SNB v1 scale_factor")
    keys = _READ_KEYS + _SHORT_KEYS + ([] if read_only else _UPDATE_KEYS)
    if any(config.get(key, "").lower() not in {"true", "false"} for key in keys):
        raise ValueError("driver_config must explicitly set all query/short-read and applicable update enable flags to true or false")
    if not any(config[key].lower() == "true" for key in _READ_KEYS):
        raise ValueError("driver_config must enable at least one complex read query")
    if not read_only and not any(config[key].lower() == "true" for key in _UPDATE_KEYS):
        raise ValueError("Mixed mode requires at least one enabled update operation in driver_config")
    if any(config[key].lower() == "true" for key in _SHORT_KEYS):
        try:
            value = float(config.get(_PREFIX + "short_read_dissipation", ""))
            if not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError()
        except ValueError as exc:
            raise ValueError("driver_config requires short_read_dissipation in 0..1 for enabled short reads") from exc


def _java_value(value: str) -> str:
    # ASCII properties preserve arbitrary Unicode and Windows paths for Java's
    # Properties.load(InputStream), which reads ISO-8859-1.
    result = ""
    encoded = value.encode("utf-16-be")
    for byte_pair in [encoded[i:i+2] for i in range(0, len(encoded), 2)]:
        number = int.from_bytes(byte_pair, "big")
        character = chr(number)
        if 32 <= number < 127:
            result += ("\\" if character in "\\ :=#!" else "") + character
        else:
            result += f"\\u{number:04x}"
    return result


def _check_inventory(root: Path, copies: dict[str, Path], directories: tuple[str, ...], pattern: str) -> None:
    """Keep native directory discovery consistent with the current manifest."""
    expected = {root / relative for relative in copies}
    for directory in directories:
        for path in (root / directory).glob(pattern):
            if path not in expected:
                raise ValueError(
                    f"Unexpected native input in output: {path.relative_to(root)}; "
                    "choose a clean output_dir to avoid mixing old and current partitions"
                )


def _publish(adapter, root: Path, manifest_name: str, copies: dict[str, Path], texts: dict[str, str], parameters: dict, extra: dict) -> GenerationResult:
    before = {relative: signature(path) for relative, path in copies.items()}
    if before != parameters["source_files"]:
        raise ValueError("LDBC source changed after validation; retry with stable inputs")
    with TemporaryDirectory(prefix="driftbench-ldbc-") as temporary:
        staged = Path(temporary)
        for relative, source in copies.items():
            target = staged / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            if signature(target) != before[relative]:
                raise ValueError("LDBC source changed during staging; retry with stable inputs")
        if before != {relative: signature(path) for relative, path in copies.items()}:
            raise ValueError("LDBC source changed during staging; retry with stable inputs")
        for relative, text in texts.items():
            target = staged / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        for relative in [*copies, *texts]:
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(staged / relative, target)
    files = [*copies, *texts]
    metadata = adapter._write_manifest(root / manifest_name, {
        "benchmark": "ldbc", "suite": _SUITE, "artifact_type": adapter.artifact_type,
        "datagen_revision": _DATAGEN_REVISION, "driver_revision": _DRIVER_REVISION,
        "source_files": before, "files": files,
        "note": "Stages supplied inputs and driver configuration. No graph generation, database execution, referential-integrity or conformance validation.",
        **extra,
    }, parameters, root=root)
    return adapter._result(root, [root / path for path in files], metadata)


@dataclass
class LDBCData(BenchmarkArtifact):
    source_dir: str | Path | None = None
    format: str = "csv_merge_foreign"
    benchmark_version: str = _SUITE
    date_format: str = "epoch_millis"
    benchmark: ClassVar[str] = "ldbc"
    artifact_type: ClassVar[str] = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        _force(force)
        if self.format != "csv_merge_foreign" or self.benchmark_version != _SUITE or self.date_format not in ("epoch_millis", "iso8601"):
            raise ValueError("Supported layout: snb-interactive-v1, csv_merge_foreign, date_format epoch_millis or iso8601")
        source = source_directory(self.source_dir, "source_dir (social_network with static/ and dynamic/)")
        copies = {}
        provenance = {}
        counts = {}
        for family, header in _HEADERS.items():
            directory, stem = family.split("/")
            matches = sorted((source / directory).glob(f"{stem}_*.csv"))
            matches = [path for path in matches if re.fullmatch(re.escape(stem) + r"_\d+_\d+\.csv", path.name)]
            if not matches:
                raise ValueError(f"Missing SNB v1 file family {family}_<partition>_<part>.csv")
            count = 0
            for path in matches:
                relative = path.relative_to(source).as_posix()
                path = source_file(source, relative)
                provenance[f"ldbc/data/social_network/{relative}"] = signature(path)
                count += _rows(path, header, dates=self.date_format)
                copies[f"ldbc/data/social_network/{relative}"] = path
            counts[family] = count
        if counts["dynamic/person"] == 0:
            raise ValueError("The SNB dataset must include at least one person row")
        if provenance != {relative: signature(path) for relative, path in copies.items()}:
            raise ValueError("LDBC source changed during validation; retry with stable inputs")
        parameters = {"source_dir": str(source), "format": self.format, "benchmark_version": self.benchmark_version, "date_format": self.date_format, "source_files": provenance}
        manifest_name = "ldbc/data/ldbc_data_manifest.json"
        root = checked_output(self, output_dir, [*copies, manifest_name], (source,))
        _check_inventory(root, copies, ("ldbc/data/social_network/static", "ldbc/data/social_network/dynamic"), "*.csv")
        if not force:
            cached = self._load_existing(root / manifest_name, root, parameters)
            if cached is not None:
                return cached
        return _publish(self, root, manifest_name, copies, {}, parameters, {"mode": "import", "format": self.format, "date_format": self.date_format, "tables": counts, "partition_files": len(copies)})


@dataclass
class LDBCQueries(BenchmarkArtifact):
    parameters_dir: str | Path | None = None
    driver_config: str | Path | None = None
    updates_dir: str | Path | None = None
    read_only: bool = True
    benchmark: ClassVar[str] = "ldbc"
    artifact_type: ClassVar[str] = "queries"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        _force(force)
        if type(self.read_only) is not bool:
            raise ValueError("read_only must be a boolean")
        source = source_directory(self.parameters_dir, "parameters_dir (14 SNB v1 substitution files)")
        if self.driver_config is None or not isinstance(self.driver_config, (str, Path)) or not str(self.driver_config).strip():
            raise ValueError("driver_config must name your existing native SNB v1 .properties file with a db implementation")
        candidate = Path(self.driver_config).expanduser().absolute()
        config_path = source_file(candidate.parent.resolve(), candidate.name)
        prefix = "ldbc/queries"
        copies = {f"{prefix}/user.properties": config_path}
        provenance = {f"{prefix}/user.properties": signature(config_path)}
        config = _properties(config_path)
        _validate_config(config, self.read_only)
        for number, header in _PARAMETERS.items():
            name = f"interactive_{number}_param.txt"
            path = source_file(source, name)
            provenance[f"{prefix}/substitution_parameters/{name}"] = signature(path)
            _rows(path, header, parameters=True)
            copies[f"{prefix}/substitution_parameters/{name}"] = path
        sources = [source, config_path]
        update_target = "empty-updates" if self.read_only else "updates"
        if self.read_only and self.updates_dir is not None:
            raise ValueError("updates_dir is only used with read_only=False")
        if not self.read_only:
            updates = source_directory(self.updates_dir, "updates_dir (matching forum/person streams for mixed mode)")
            streams = {}
            for path in sorted(updates.glob("updateStream_*.csv")):
                match = re.fullmatch(r"updateStream_(\d+_\d+)_(forum|person)\.csv", path.name)
                if match:
                    streams.setdefault(match[1], set()).add(match[2])
                    path = source_file(updates, path.name)
                    copies[f"{prefix}/updates/{path.name}"] = path
                    provenance[f"{prefix}/updates/{path.name}"] = signature(path)
            if not streams or any(types != {"forum", "person"} for types in streams.values()):
                raise ValueError("updates_dir must contain matching nonempty updateStream_<partition>_<part>_forum.csv and _person.csv pairs")
            sources.append(updates)
        if provenance != {relative: signature(path) for relative, path in copies.items()}:
            raise ValueError("LDBC source changed during validation; retry with stable inputs")
        parameters = {"read_only": self.read_only, "parameters_dir": str(source), "driver_config": str(config_path), "updates_dir": str(sources[-1]) if not self.read_only else None, "source_files": provenance, "driver_revision": _DRIVER_REVISION}
        manifest_name = f"{prefix}/ldbc_queries_manifest.json"
        text_names = [f"{prefix}/overrides.properties", f"{prefix}/command.json", f"{prefix}/README.md"]
        if self.read_only:
            text_names.append(f"{prefix}/empty-updates/README.txt")
        root = checked_output(self, output_dir, [*copies, *text_names, manifest_name], tuple(sources))
        _check_inventory(root, copies, (f"{prefix}/{update_target}",), "updateStream*")
        # Absolute paths in native properties are part of the local cache identity.
        parameters["output_dir"] = str(root)
        if not force:
            cached = self._load_existing(root / manifest_name, root, parameters)
            if cached is not None:
                return cached
        overrides = {"workload": _WORKLOAD, _PREFIX + "parameters_dir": (root / prefix / "substitution_parameters").as_posix(), _PREFIX + "updates_dir": (root / prefix / update_target).as_posix()}
        if self.read_only:
            overrides.update({key: "false" for key in _UPDATE_KEYS})
        import json
        texts = {
            f"{prefix}/overrides.properties": "# FIRST -P file wins in the SNB Interactive v1.2.0 driver.\n" + "".join(f"{key}={_java_value(value)}\n" for key, value in overrides.items()),
            f"{prefix}/command.json": json.dumps({"schema": "driftbench.ldbc-command/v1", "argv": ["java", "-cp", "<driver-and-db-implementation-classpath>", "org.ldbcouncil.snb.driver.Client", "-P", str(root / prefix / "overrides.properties"), "-P", str(root / prefix / "user.properties")], "cwd": str(config_path.parent), "executed": False, "requires": "Set the classpath for SNB driver v1.2.0 and your DB implementation; load the matching dataset first."}, indent=2) + "\n",
            f"{prefix}/README.md": "# SNB Interactive v1 driver handoff\n\nThe native user.properties is preserved byte-for-byte and may contain your local credentials. Keep it private.\nUse the argv in command.json after replacing the classpath placeholder. Its cwd preserves relative paths in your original config. The first -P file has priority.\nAll 14 parameter files are included. " + ("All update operations are disabled.\n" if self.read_only else "User operation-enable settings are preserved; update streams are copied.\n") + "Database schema, implementation, load/measurement and conformance checks remain external. Graph data and parameter/stream scale must match your configuration.\nRegenerate after moving the output directory: overrides use absolute local paths.\n",
        }
        if self.read_only:
            texts[f"{prefix}/empty-updates/README.txt"] = "No updateStream CSV files: read-only handoff.\n"
        return _publish(self, root, manifest_name, copies, texts, parameters, {"mode": "driver_handoff", "read_only": self.read_only, "scale_factor": config[_PREFIX + "scale_factor"], "parameter_files": 14})


def data(source_dir: str | Path | None = None, format: str = "csv_merge_foreign", benchmark_version: str = _SUITE, date_format: str = "epoch_millis") -> LDBCData:
    """Stage the 20 SNB v1 CSV families; declare the source date representation."""
    return LDBCData(source_dir, format, benchmark_version, date_format)


def queries(parameters_dir: str | Path | None = None, driver_config: str | Path | None = None, updates_dir: str | Path | None = None, read_only: bool = True) -> LDBCQueries:
    """Stage real parameters and a supplied DB-specific native driver config."""
    return LDBCQueries(parameters_dir, driver_config, updates_dir, read_only)
