from atproto import AtUri, CAR, models
import sqlite3
from typing import Optional


def get_create_post_record(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> Optional[list[object]]:
    # comments below for sanity
    # decode CAR file for the commit
    # commits.blocks is the CAR file containing relevant blocks, as a diff since the previous repo state
    car = CAR.from_bytes(commit.blocks)
    # loop through each RepoOp in the commit
    create_post_records = []
    for op in commit.ops:
        # check if the RepoOp action is "create" (not delete or update)
        if op.action == "create":
            # ensures there is a CID (Content ID) associated with the create operation
            if not op.cid:
                continue
            # get data for the block using the CID
            record_raw_data = car.blocks.get(op.cid)
            # ensures there is data in the create operation
            if not record_raw_data:
                continue
            # convert the raw operation data into a feed post record
            record = models.get_or_create(record_raw_data, models.ids.AppBskyFeedPost, strict=True)
            # verify the model of the record is the feed post record model
            if models.is_record_type(record, models.ids.AppBskyFeedPost):
                # build the uri from the available data so far
                uri = AtUri.from_str(f"at://{commit.repo}/{op.path}")
                processed_record = {
                    "did": commit.repo,
                    "uri": str(uri),
                    "cid": str(op.cid),
                    "record": record,
                }
                create_post_records.append(processed_record)
    return create_post_records


def insert_commit(processed_commit: object, con: sqlite3.Connection) -> None:
    post_did: str = processed_commit["did"]
    post_cid: str = processed_commit["cid"]
    post_uri: str = processed_commit["uri"]
    post_record: models.AppBskyFeedPost.Record = processed_commit["record"]
    insert = [
        (
            post_did,
            post_cid,
            post_uri,
            post_record,
        )
    ]
    firehose_raw_insert(con, insert)


def sqlite_setup() -> None:
    con = sqlite3.connect("./data/test.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS firehose_raw;")
    cur.execute(
        """CREATE TABLE IF NOT EXISTS firehose_raw (
        uri TEXT,
        did TEXT,
        cid TEXT,
        record TEXT);"""
    )
    con.commit()
    con.close()


def firehose_raw_insert(con: sqlite3.Connection, insert_data: list) -> None:
    try:
        cur = con.cursor()
        cur.executemany("INSERT INTO firehose_raw VALUES(?, ?, ?, ?, ?, ?, ?, ?);", insert_data)
        con.commit()
    except sqlite3.IntegrityError as e:
        print(e)


def firehose_raw_size(con: sqlite3.Connection) -> int:
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM firehose_raw;")
    con.commit()
    res = cur.fetchone()[0]
    if res is None:
        res = 0
    return res


if __name__ == "__main__":
    pass
