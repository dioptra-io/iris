from iris.worker.pipeline import extract_round_number


def test_extract_round_number():
    file_name = "ddd8541d-b4f5-42ce-b163-e3e9bfcd0a47_starttime_10.log"
    assert extract_round_number(file_name) == 10

    file_name = "ddd8541d-b4f5-42ce-b163-e3e9bfcd0a47_starttime_1.log"
    assert extract_round_number(file_name) == 1

    file_name = "ddd8541d-b4f5-42ce-b163-e3e9bfcd0a47_results_2.csv"
    assert extract_round_number(file_name) == 2

    file_name = "ddd8541d-b4f5-42ce-b163-e3e9bfcd0a47_next_round_csv_4.csv"
    assert extract_round_number(file_name) == 4

    file_name = "ddd8541d-b4f5-42ce-b163-e3e9bfcd0a47_csv_5.csv"
    assert extract_round_number(file_name) == 5

    file_name = "ddd8541d-b4f5-42ce-b163-e3e9bfcd0a47_shuffled_next_round_csv_7.csv"
    assert extract_round_number(file_name) == 7
