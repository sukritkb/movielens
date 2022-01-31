from src.common.utils import Utils


def test_get_join_conditions():
    data = (
        ["x", "y", "z"],
        "target.x=updates.x and target.y=updates.y and target.z=updates.z",
    )
    res = Utils.get_join_conditions(data[0])
    assert res == data[1]


def test_modify_set():
    column_set = {
        "userId": "userId",
        "movieId": "movieId",
        "rating": "rating",
        "timestamp": "timestamp",
    }
    expected = {
        "userId": "updates.userId",
        "movieId": "updates.movieId",
        "rating": "updates.rating",
        "timestamp": "updates.timestamp",
    }

    res = Utils.modify_upsert_set(column_set)
    assert res == expected
