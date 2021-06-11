from iris.commons.round import Round


def test_round():
    round = Round.decode("1:5:0")

    assert round.number == 1
    assert round.limit == 5
    assert round.offset == 0

    assert round.encode() == "1:5:0"

    assert round.min_ttl == 1
    assert round.max_ttl == 5


def test_round_from_filename():
    round = Round.decode_from_filename(
        "81484af7-6776-42a7-80fd-cb23b73855f8_results_1:0:0.csv.zst_00"
    )
    assert round.number == 1
    assert round.limit == 0
    assert round.offset == 0

    round = Round.decode_from_filename(
        "ddd8541d-b4f5-42ce-b163-e3e9bfcd0a47_results_2:6:8.csv"
    )
    assert round.number == 2
    assert round.limit == 6
    assert round.offset == 8

    round = Round.decode_from_filename(
        "ddd8541d-b4f5-42ce-b163-e3e9bfcd0a47_next_round_csv_4:3:5.csv"
    )
    assert round.number == 4
    assert round.limit == 3
    assert round.offset == 5


def test_next_round():
    # Round 1
    round = Round(1, 10, 0)
    round.next_round() == Round(2, 0, 0)
    round.next_round(30) == Round(1, 10, 1)

    # [1..10], [11..20], ...
    round.next_round(9) == Round(2, 0, 0)
    round.next_round(10) == Round(2, 0, 0)
    round.next_round(11) == Round(1, 10, 1)

    # Round 1 no sliding window
    round = Round(1, 0, 0)
    round.next_round() == Round(2, 0, 0)

    # Round > 1
    round = Round(2, 0, 0)
    round.next_round(30) == Round(3, 0, 0)
