"""Small local SNB v1 layout fixtures; no native tools or database involved."""
from __future__ import annotations

from pathlib import Path


HEADERS = {
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
PARAMETER_ROWS = {
    1: ("personId|firstName", "1|Ada"),
    2: ("personId|maxDate", "1|1325376000000"),
    3: ("personId|startDate|durationDays|countryXName|countryYName", "1|1325376000000|30|Germany|France"),
    4: ("personId|startDate|durationDays", "1|1325376000000|30"),
    5: ("personId|minDate", "1|1325376000000"),
    6: ("personId|tagName", "1|Music"),
    7: ("personId", "1"), 8: ("personId", "1"),
    9: ("personId|maxDate", "1|1325376000000"),
    10: ("personId|month", "1|4"),
    11: ("personId|countryName|workFromYear", "1|Germany|2010"),
    12: ("personId|tagClassName", "1|MusicalArtist"),
    13: ("person1Id|person2Id", "1|2"), 14: ("person1Id|person2Id", "1|2"),
}
PREFIX = "ldbc.snb.interactive."
UPDATES = ["1AddPerson", "2AddPostLike", "3AddCommentLike", "4AddForum", "5AddForumMembership", "6AddPost", "7AddComment", "8AddFriendship"]
SHORTS = ["1PersonProfile", "2PersonPosts", "3PersonFriends", "4MessageContent", "5MessageCreator", "6MessageForum", "7MessageReplies"]
PRIVATE_DUMMY = "private-dummy-value-not-a-real-credential"


def write_data(root: Path, *, date_format: str = "epoch_millis") -> Path:
    # Hadoop Datagen StringDateFormatter emits a date-only birthday and a
    # yyyy-MM-dd'T'HH:mm:ss.SSSZ timestamp, including the native +0000 offset.
    birthday, created = ("631152000000", "1325376000000") if date_format == "epoch_millis" else ("1990-01-01", "2012-01-01T00:00:00.000+0000")
    for family, header in HEADERS.items():
        path = root / f"{family}_0_0.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        row = f"1|Ada|Lovelace|female|{birthday}|{created}|127.0.0.1|Firefox|1\n" if family == "dynamic/person" else ""
        path.write_text(header + "\n" + row, encoding="utf-8")
    return root


def write_parameters(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    for number, (header, row) in PARAMETER_ROWS.items():
        (root / f"interactive_{number}_param.txt").write_text(header + "\n" + row + "\n", encoding="utf-8")
    return root


def config_values() -> dict[str, str]:
    return {
        "db": "example.local.Database",
        "mode": "execute_benchmark",
        "operation_count": "100",
        "thread_count": "2",
        "time_compression_ratio": "1.0",
        "password": PRIVATE_DUMMY,
        PREFIX + "scale_factor": "1",
        **{PREFIX + f"LdbcQuery{i}_enable": "true" for i in range(1, 15)},
        **{PREFIX + f"LdbcShortQuery{name}_enable": "false" for name in SHORTS},
        **{PREFIX + f"LdbcUpdate{name}_enable": "true" for name in UPDATES},
    }


def write_config(path: Path, changes: dict[str, str | None] | None = None) -> Path:
    values = config_values()
    for name, value in (changes or {}).items():
        if value is None:
            values.pop(name, None)
        else:
            values[name] = value
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(("# Native user fixture; retain exact CRLF bytes.\r\n" + "".join(f"{name}={value}\r\n" for name, value in values.items())).encode("iso-8859-1"))
    return path


def write_updates(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    # Update streams are opaque native payloads to the staging adapter. These
    # small records test byte preservation, not execution or SNB conformance.
    (root / "updateStream_0_0_person.csv").write_text("1325376000000|1325375999000|8|1|2|1325376000000\n", encoding="utf-8")
    (root / "updateStream_0_0_forum.csv").write_text("1325376000000|1325375999000|4|1|Forum|1325376000000|1|2\n", encoding="utf-8")
    return root
