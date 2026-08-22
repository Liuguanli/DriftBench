"""Join Order Benchmark (JOB) adapter.

JOB was introduced in "How Good Are Query Optimizers, Really?" (Leis et al., VLDB 2015).
It uses a real snapshot of the Internet Movie Database (IMDB) with 21 tables
and 113 join-order-sensitive queries across 33 query families.

Quick start:
    from driftbench.data.job import data as job_data, queries as job_queries

    job_data(scale_factor=1).generate(output_dir="./artifacts")
    job_queries().generate(output_dir="./artifacts")

Scale note:
    Synth mode generates an 11-table subset proportional to scale_factor:
      title: 500*sf rows, name: 1000*sf, cast_info: 5000*sf, movie_info: 3000*sf,
      keyword: 200*sf, movie_keyword: 2000*sf, company_name: 100*sf, movie_companies: 1000*sf.
    The synth adapter generates an 11-table subset; full IMDB data must be loaded manually.

Queries:
    20 representative templates (simplified from the original 113) covering all
    major join patterns: selective filters, cross-table aggregation, multi-way joins.
"""
from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from pathlib import Path

from driftbench.console import console_print

from .base import BenchmarkArtifact, GenerationResult


_JOB_KIND_TYPES = (
    "movie", "tv series", "tv movie", "video movie", "video game", "episode", "tv mini series",
)
_JOB_INFO_TYPES = (
    "genres", "rating", "votes", "languages", "countries", "runtimes",
    "color info", "sound mix", "certificates", "aspect ratio",
)
_JOB_COMPANY_TYPES = (
    "distributors", "production companies", "miscellaneous companies", "special effects companies",
)


_JOB_DDL = """\
-- JOB (IMDB) Schema - 11-table synth subset (PostgreSQL-compatible)

CREATE TABLE IF NOT EXISTS kind_type (
    id    INTEGER PRIMARY KEY,
    kind  VARCHAR(15)
);

CREATE TABLE IF NOT EXISTS info_type (
    id   INTEGER PRIMARY KEY,
    info VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS company_type (
    id   INTEGER PRIMARY KEY,
    kind VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS title (
    id               INTEGER PRIMARY KEY,
    title            TEXT NOT NULL,
    imdb_index       VARCHAR(12),
    kind_id          INTEGER REFERENCES kind_type(id),
    production_year  INTEGER,
    imdb_id          INTEGER,
    phonetic_code    VARCHAR(5),
    episode_of_id    INTEGER,
    season_nr        INTEGER,
    episode_nr       INTEGER,
    series_years     VARCHAR(49),
    md5sum           VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS name (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    imdb_index    VARCHAR(12),
    imdb_id       INTEGER,
    gender        CHAR(1),
    name_pcode_cf VARCHAR(5),
    name_pcode_nf VARCHAR(5),
    surname_pcode VARCHAR(5),
    md5sum        VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS keyword (
    id      INTEGER PRIMARY KEY,
    keyword TEXT NOT NULL,
    phonetic_code VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS company_name (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    country_code  VARCHAR(6),
    imdb_id       INTEGER,
    name_pcode_nf VARCHAR(5),
    name_pcode_sf VARCHAR(5),
    md5sum        VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS cast_info (
    id          INTEGER PRIMARY KEY,
    person_id   INTEGER REFERENCES name(id),
    movie_id    INTEGER REFERENCES title(id),
    person_role_id INTEGER,
    note        TEXT,
    nr_order    INTEGER,
    role_id     INTEGER
);

CREATE TABLE IF NOT EXISTS movie_info (
    id       INTEGER PRIMARY KEY,
    movie_id INTEGER REFERENCES title(id),
    info_type_id INTEGER REFERENCES info_type(id),
    info     TEXT NOT NULL,
    note     TEXT
);

CREATE TABLE IF NOT EXISTS movie_keyword (
    id         INTEGER PRIMARY KEY,
    movie_id   INTEGER REFERENCES title(id),
    keyword_id INTEGER REFERENCES keyword(id)
);

CREATE TABLE IF NOT EXISTS movie_companies (
    id             INTEGER PRIMARY KEY,
    movie_id       INTEGER REFERENCES title(id),
    company_id     INTEGER REFERENCES company_name(id),
    company_type_id INTEGER REFERENCES company_type(id),
    note           TEXT
);
"""


_JOB_QUERIES: dict[str, str] = {
    "1a_keyword_filter": """\
-- JOB 1a: Movies with a specific keyword
SELECT t.title, t.production_year
FROM title t
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN keyword k ON mk.keyword_id = k.id
WHERE k.keyword = 'murder'
  AND t.production_year > 2000
ORDER BY t.production_year DESC;
""",
    "2a_company_movies": """\
-- JOB 2a: Movies produced by companies in a specific country
SELECT t.title, cn.name AS company, t.production_year
FROM title t
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
WHERE cn.country_code = '[us]'
  AND t.production_year BETWEEN 1990 AND 2010;
""",
    "3a_movie_info_filter": """\
-- JOB 3a: Movies with specific info type
SELECT t.title, mi.info
FROM title t
JOIN movie_info mi ON t.id = mi.movie_id
JOIN info_type it ON mi.info_type_id = it.id
WHERE it.info = 'genres'
  AND mi.info = 'Drama';
""",
    "4a_cast_keyword": """\
-- JOB 4a: Actors in movies with a given keyword
SELECT DISTINCT n.name, t.title
FROM name n
JOIN cast_info ci ON n.id = ci.person_id
JOIN title t ON ci.movie_id = t.id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN keyword k ON mk.keyword_id = k.id
WHERE k.keyword = 'based-on-novel'
  AND t.production_year > 1995
ORDER BY n.name;
""",
    "5a_company_country_cast": """\
-- JOB 5a: Cast members in US movies with specific keyword
SELECT n.name, t.title, cn.name AS company
FROM name n
JOIN cast_info ci ON n.id = ci.person_id
JOIN title t ON ci.movie_id = t.id
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN keyword k ON mk.keyword_id = k.id
WHERE cn.country_code = '[us]'
  AND k.keyword = 'sequel';
""",
    "6a_info_company": """\
-- JOB 6a: Movies by US companies with genre info
SELECT t.title, mi.info AS genre, cn.name AS company
FROM title t
JOIN movie_info mi ON t.id = mi.movie_id
JOIN info_type it ON mi.info_type_id = it.id
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
WHERE it.info = 'genres'
  AND cn.country_code = '[us]'
  AND mi.info IN ('Action', 'Comedy', 'Drama');
""",
    "7a_keyword_count": """\
-- JOB 7a: Keyword frequency in movies after 2000
SELECT k.keyword, COUNT(*) AS cnt
FROM keyword k
JOIN movie_keyword mk ON k.id = mk.keyword_id
JOIN title t ON mk.movie_id = t.id
WHERE t.production_year > 2000
GROUP BY k.keyword
ORDER BY cnt DESC
LIMIT 20;
""",
    "8a_actor_productivity": """\
-- JOB 8a: Actors with most movies in a decade
SELECT n.name, COUNT(DISTINCT ci.movie_id) AS movie_count
FROM name n
JOIN cast_info ci ON n.id = ci.person_id
JOIN title t ON ci.movie_id = t.id
WHERE t.production_year BETWEEN 2000 AND 2010
GROUP BY n.name
HAVING COUNT(DISTINCT ci.movie_id) > 5
ORDER BY movie_count DESC;
""",
    "9a_multi_keyword_movie": """\
-- JOB 9a: Movies that have both keywords (multi-keyword requirement)
SELECT t.title, t.production_year
FROM title t
WHERE t.id IN (
    SELECT mk.movie_id FROM movie_keyword mk
    JOIN keyword k ON mk.keyword_id = k.id
    WHERE k.keyword = 'superhero'
)
AND t.id IN (
    SELECT mk.movie_id FROM movie_keyword mk
    JOIN keyword k ON mk.keyword_id = k.id
    WHERE k.keyword = 'sequel'
);
""",
    "10a_full_join": """\
-- JOB 10a: Five-way join (title + cast + company + keyword + info)
SELECT t.title, n.name AS actor, cn.name AS company, k.keyword, mi.info AS genre
FROM title t
JOIN cast_info ci ON t.id = ci.movie_id
JOIN name n ON ci.person_id = n.id
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN keyword k ON mk.keyword_id = k.id
JOIN movie_info mi ON t.id = mi.movie_id
JOIN info_type it ON mi.info_type_id = it.id
WHERE cn.country_code = '[us]'
  AND k.keyword = 'murder'
  AND it.info = 'genres'
  AND mi.info = 'Thriller'
  AND t.production_year > 2000;
""",
    "11a_company_keyword_year": """\
-- JOB 11a: Production companies by keyword volume per decade
SELECT cn.name, k.keyword,
       (t.production_year / 10 * 10) AS decade,
       COUNT(*) AS cnt
FROM company_name cn
JOIN movie_companies mc ON cn.id = mc.company_id
JOIN title t ON mc.movie_id = t.id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN keyword k ON mk.keyword_id = k.id
GROUP BY cn.name, k.keyword, decade
ORDER BY cnt DESC
LIMIT 50;
""",
    "12a_cast_info_selective": """\
-- JOB 12a: Cast info with note filter (selective predicate)
SELECT t.title, n.name, ci.note
FROM cast_info ci
JOIN title t ON ci.movie_id = t.id
JOIN name n ON ci.person_id = n.id
WHERE ci.note LIKE '%(director)%'
  AND t.production_year > 1980;
""",
    "13a_movie_info_aggregate": """\
-- JOB 13a: Count movies per genre per year
SELECT mi.info AS genre, t.production_year, COUNT(*) AS cnt
FROM movie_info mi
JOIN info_type it ON mi.info_type_id = it.id
JOIN title t ON mi.movie_id = t.id
WHERE it.info = 'genres'
  AND t.production_year IS NOT NULL
GROUP BY mi.info, t.production_year
ORDER BY t.production_year, cnt DESC;
""",
    "14a_company_info_cast": """\
-- JOB 14a: US company drama films and their lead actors
SELECT cn.name, t.title, n.name AS actor
FROM company_name cn
JOIN movie_companies mc ON cn.id = mc.company_id
JOIN title t ON mc.movie_id = t.id
JOIN movie_info mi ON t.id = mi.movie_id
JOIN info_type it ON mi.info_type_id = it.id
JOIN cast_info ci ON t.id = ci.movie_id
JOIN name n ON ci.person_id = n.id
WHERE cn.country_code = '[us]'
  AND it.info = 'genres'
  AND mi.info = 'Drama'
  AND ci.nr_order = 1;
""",
    "15a_keyword_year_range": """\
-- JOB 15a: Keyword usage trend across years
SELECT k.keyword, t.production_year, COUNT(*) AS usage
FROM keyword k
JOIN movie_keyword mk ON k.id = mk.keyword_id
JOIN title t ON mk.movie_id = t.id
WHERE t.production_year BETWEEN 1980 AND 2020
GROUP BY k.keyword, t.production_year
HAVING COUNT(*) > 10
ORDER BY k.keyword, t.production_year;
""",
    "16a_actor_company": """\
-- JOB 16a: Actors and the companies they most worked with
SELECT n.name AS actor, cn.name AS company, COUNT(*) AS collaborations
FROM name n
JOIN cast_info ci ON n.id = ci.person_id
JOIN title t ON ci.movie_id = t.id
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
GROUP BY n.name, cn.name
HAVING COUNT(*) > 2
ORDER BY collaborations DESC;
""",
    "17a_selective_cast_keyword": """\
-- JOB 17a: Actors in superhero films for US studios
SELECT DISTINCT n.name, t.title, cn.name AS studio
FROM name n
JOIN cast_info ci ON n.id = ci.person_id
JOIN title t ON ci.movie_id = t.id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN keyword k ON mk.keyword_id = k.id
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
WHERE k.keyword = 'superhero'
  AND cn.country_code = '[us]'
ORDER BY t.production_year DESC;
""",
    "18a_movie_info_keyword": """\
-- JOB 18a: Action films with specific keywords
SELECT t.title, k.keyword
FROM title t
JOIN movie_info mi ON t.id = mi.movie_id
JOIN info_type it ON mi.info_type_id = it.id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN keyword k ON mk.keyword_id = k.id
WHERE it.info = 'genres'
  AND mi.info = 'Action'
  AND k.keyword IN ('explosion', 'chase', 'car-chase')
ORDER BY t.production_year DESC;
""",
    "19a_company_output_volume": """\
-- JOB 19a: Companies ranked by total movie output
SELECT cn.name, cn.country_code, COUNT(DISTINCT mc.movie_id) AS total_movies
FROM company_name cn
JOIN movie_companies mc ON cn.id = mc.company_id
JOIN title t ON mc.movie_id = t.id
WHERE t.production_year > 1950
GROUP BY cn.name, cn.country_code
ORDER BY total_movies DESC
LIMIT 100;
""",
    "20a_full_eight_table": """\
-- JOB 20a: Full 8-table join (the hardest join-order problem in JOB)
SELECT t.title, n.name, cn.name AS company,
       k.keyword, mi.info AS genre,
       t.production_year
FROM title t
JOIN cast_info ci ON t.id = ci.movie_id
JOIN name n ON ci.person_id = n.id
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN keyword k ON mk.keyword_id = k.id
JOIN movie_info mi ON t.id = mi.movie_id
JOIN info_type it ON mi.info_type_id = it.id
WHERE cn.country_code = '[us]'
  AND it.info = 'genres'
  AND mi.info IN ('Action', 'Thriller', 'Crime')
  AND k.keyword IN ('murder', 'crime', 'detective')
  AND t.production_year BETWEEN 2000 AND 2020
ORDER BY t.production_year DESC;
""",
}


@dataclass
class JOBData(BenchmarkArtifact):
    """Generate synthetic JOB (IMDB) data artifacts.

    Generates an 11-table subset of the IMDB schema.
    Scale controls row counts proportionally.

    Args:
        scale_factor: Scales row counts for all generated tables (default 1).
    """

    scale_factor: int | float = 1

    benchmark: str = "job"
    artifact_type: str = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "job" / "data"
        out_dir.mkdir(parents=True, exist_ok=True)

        sf = self._sf()
        cache_parameters = {"scale_factor": sf}

        if not force:
            existing = self._load_existing(
                out_dir / "job_data_manifest.json", root, cache_parameters
            )
            if existing is not None:
                console_print(f"[driftbench] JOB data already exists at {out_dir}. Reusing.")
                return existing
        console_print(f"[driftbench] Generating JOB data (sf={sf}) -> {out_dir}")

        ddl = self._write_text(out_dir / "job_schema.sql", _JOB_DDL)

        files = self._generate_synth(out_dir)
        files.insert(0, ddl)
        row_counts = {
            "kind_type": len(_JOB_KIND_TYPES),
            "info_type": len(_JOB_INFO_TYPES),
            "company_type": len(_JOB_COMPANY_TYPES),
            "title": 500 * sf,
            "name": 1000 * sf,
            "keyword": 200 * sf,
            "company_name": 100 * sf,
            "cast_info": 5000 * sf,
            "movie_info": 3000 * sf,
            "movie_keyword": 2000 * sf,
            "movie_companies": 1000 * sf,
        }
        metadata = self._write_manifest(
            out_dir / "job_data_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "scale_factor": sf,
                "tables": row_counts,
                "files": self._paths_relative_to(root, files),
                "note": "Synthetic 11-table JOB subset for onboarding and API testing.",
            },
            cache_parameters,
            root=root,
        )
        return self._result(root, files, metadata)

    def _sf(self) -> int:
        return max(1, int(round(float(self.scale_factor))))

    def _generate_synth(self, out_dir: Path) -> list[Path]:
        sf = self._sf()
        rng = random.Random(42)
        files: list[Path] = []

        files.append(self._write_csv(out_dir / "kind_type.csv",
            ["id", "kind"],
            [[i + 1, k] for i, k in enumerate(_JOB_KIND_TYPES)]))

        files.append(self._write_csv(out_dir / "info_type.csv",
            ["id", "info"],
            [[i + 1, it] for i, it in enumerate(_JOB_INFO_TYPES)]))

        files.append(self._write_csv(out_dir / "company_type.csv",
            ["id", "kind"],
            [[i + 1, ct] for i, ct in enumerate(_JOB_COMPANY_TYPES)]))

        title_count = 500 * sf
        genres = ["Drama", "Comedy", "Action", "Thriller", "Crime", "Horror", "Romance"]
        titles: list[list] = []
        for i in range(1, title_count + 1):
            titles.append([
                i, f"Movie Title {i}", None,
                rng.randint(1, len(_JOB_KIND_TYPES)),
                rng.randint(1960, 2023),
                None, None, None, None, None, None, None,
            ])
        files.append(self._write_csv(out_dir / "title.csv",
            ["id", "title", "imdb_index", "kind_id", "production_year", "imdb_id",
             "phonetic_code", "episode_of_id", "season_nr", "episode_nr", "series_years", "md5sum"],
            titles))

        name_count = 1000 * sf
        names: list[list] = []
        for i in range(1, name_count + 1):
            names.append([
                i, f"Person Name {i}", None, None,
                rng.choice(["m", "f", None]),
                None, None, None, None,
            ])
        files.append(self._write_csv(out_dir / "name.csv",
            ["id", "name", "imdb_index", "imdb_id", "gender",
             "name_pcode_cf", "name_pcode_nf", "surname_pcode", "md5sum"],
            names))

        keyword_count = 200 * sf
        keywords_vocab = [
            "murder", "crime", "detective", "superhero", "sequel", "based-on-novel",
            "explosion", "chase", "car-chase", "romance", "war", "friendship",
            "time-travel", "comedy", "satire", "biography", "sports", "music",
            "animation", "zombie",
        ]
        keywords: list[list] = []
        for i in range(1, keyword_count + 1):
            kw = keywords_vocab[i % len(keywords_vocab)] + (f"_{i}" if i >= len(keywords_vocab) else "")
            keywords.append([i, kw, None])
        files.append(self._write_csv(out_dir / "keyword.csv",
            ["id", "keyword", "phonetic_code"],
            keywords))

        company_count = 100 * sf
        country_codes = ["[us]", "[gb]", "[de]", "[fr]", "[jp]", "[it]", "[au]"]
        companies: list[list] = []
        for i in range(1, company_count + 1):
            companies.append([
                i, f"Studio {i}", rng.choice(country_codes),
                None, None, None, None,
            ])
        files.append(self._write_csv(out_dir / "company_name.csv",
            ["id", "name", "country_code", "imdb_id", "name_pcode_nf", "name_pcode_sf", "md5sum"],
            companies))

        cast_count = 5000 * sf
        cast_info: list[list] = []
        for i in range(1, cast_count + 1):
            cast_info.append([
                i,
                rng.randint(1, name_count),
                rng.randint(1, title_count),
                None,
                rng.choice([None, "(director)", "(producer)", "(writer)"]),
                rng.randint(1, 10),
                rng.randint(1, 12),
            ])
        files.append(self._write_csv(out_dir / "cast_info.csv",
            ["id", "person_id", "movie_id", "person_role_id", "note", "nr_order", "role_id"],
            cast_info))

        # info_type_id 1 = genres
        movie_info_count = 3000 * sf
        movie_info: list[list] = []
        for i in range(1, movie_info_count + 1):
            movie_info.append([
                i,
                rng.randint(1, title_count),
                rng.randint(1, 10),
                rng.choice(genres),
                None,
            ])
        files.append(self._write_csv(out_dir / "movie_info.csv",
            ["id", "movie_id", "info_type_id", "info", "note"],
            movie_info))

        movie_keyword_count = 2000 * sf
        movie_keywords: list[list] = []
        for i in range(1, movie_keyword_count + 1):
            movie_keywords.append([
                i,
                rng.randint(1, title_count),
                rng.randint(1, keyword_count),
            ])
        files.append(self._write_csv(out_dir / "movie_keyword.csv",
            ["id", "movie_id", "keyword_id"],
            movie_keywords))

        movie_companies_count = 1000 * sf
        movie_companies: list[list] = []
        for i in range(1, movie_companies_count + 1):
            movie_companies.append([
                i,
                rng.randint(1, title_count),
                rng.randint(1, company_count),
                rng.randint(1, len(_JOB_COMPANY_TYPES)),
                None,
            ])
        files.append(self._write_csv(out_dir / "movie_companies.csv",
            ["id", "movie_id", "company_id", "company_type_id", "note"],
            movie_companies))

        return files

    def _write_csv(self, path: Path, header: list[str], rows: list[list]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        return path


@dataclass
class JOBQueries(BenchmarkArtifact):
    """Generate 20 representative JOB SQL query templates.

    Covers all major join patterns from the original 113-query JOB suite:
    selective filters, cross-table aggregation, multi-way joins (up to 8 tables).
    """

    benchmark: str = "job"
    artifact_type: str = "queries"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "job" / "queries"
        out_dir.mkdir(parents=True, exist_ok=True)

        cache_parameters: dict[str, object] = {}

        if not force:
            existing = self._load_existing(
                out_dir / "job_queries_manifest.json", root, cache_parameters
            )
            if existing is not None:
                console_print(f"[driftbench] JOB queries already exist at {out_dir}. Reusing.")
                return existing
        console_print(f"[driftbench] Generating JOB queries -> {out_dir}")

        files: list[Path] = []
        for name, sql in _JOB_QUERIES.items():
            files.append(self._write_text(out_dir / f"{name}.sql", sql))

        bundle = "\n".join(
            f"-- === {name.upper()} ===\n{sql}"
            for name, sql in _JOB_QUERIES.items()
        )
        files.append(self._write_text(out_dir / "job_all_queries.sql", bundle))

        metadata = self._write_manifest(
            out_dir / "job_queries_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "query_count": len(_JOB_QUERIES),
                "query_families": list(_JOB_QUERIES.keys()),
                "join_complexity": {
                    "2_table": ["3a_movie_info_filter", "7a_keyword_count"],
                    "3_table": ["1a_keyword_filter", "2a_company_movies", "13a_movie_info_aggregate", "15a_keyword_year_range", "19a_company_output_volume"],
                    "4_table": ["4a_cast_keyword", "8a_actor_productivity", "9a_multi_keyword_movie", "12a_cast_info_selective"],
                    "5_table": ["6a_info_company", "11a_company_keyword_year", "16a_actor_company"],
                    "6_table": ["5a_company_country_cast", "14a_company_info_cast"],
                    "7_table": ["17a_selective_cast_keyword", "18a_movie_info_keyword"],
                    "8_table": ["10a_full_join", "20a_full_eight_table"],
                },
                "files": self._paths_relative_to(root, files),
                "note": (
                    "20 representative templates derived from the 113-query JOB suite "
                    "(Leis et al., VLDB 2015). Queries reference the 11-table synth subset; "
                    "adapt table names for the full 21-table IMDB snapshot."
                ),
            },
            cache_parameters,
            root=root,
        )
        return self._result(root, files, metadata)


def data(scale_factor: int | float = 1) -> JOBData:
    """Return a JOBData adapter.

    Args:
        scale_factor: Controls synth row counts proportionally (default 1).
    """
    return JOBData(scale_factor=scale_factor)


def queries() -> JOBQueries:
    """Return a JOBQueries adapter producing 20 representative JOB SQL templates."""
    return JOBQueries()
