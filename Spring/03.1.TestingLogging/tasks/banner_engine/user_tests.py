import typing

import pytest

from banner_engine import (
    BannerStat, BannerStorage, Banner, EpsilonGreedyBannerEngine, EmptyBannerStorageError
)
import random

TEST_DEFAULT_CTR = 0.1


@pytest.fixture(scope="function")
def test_banners() -> list[Banner]:
    return [
        Banner("b1", cost=1, stat=BannerStat(10, 20)),
        Banner("b2", cost=250, stat=BannerStat(20, 20)),
        Banner("b3", cost=100, stat=BannerStat(0, 20)),
        Banner("b4", cost=100, stat=BannerStat(1, 20)),
    ]


@pytest.mark.parametrize("clicks, shows, expected_ctr", [(1, 1, 1.0), (20, 100, 0.2), (5, 100, 0.05)])
def test_banner_stat_ctr_value(clicks: int, shows: int, expected_ctr: float) -> None:
    tmp = BannerStat(clicks, shows,)
    assert tmp.compute_ctr(0) == expected_ctr


def test_empty_stat_compute_ctr_returns_default_ctr() -> None:
    tmp = BannerStat(20,0)
    assert tmp.compute_ctr(232.0) == 232.0
    assert tmp.compute_ctr(-1.0) == -1.0
    assert tmp.compute_ctr(0.0) == 0.0


def test_banner_stat_add_show_lowers_ctr() -> None:
    tmp = BannerStat(0, 0)
    tmp.add_show()
    assert tmp.shows == 1
    tmp.add_show()
    assert tmp.shows == 2


def test_banner_stat_add_click_increases_ctr() -> None:
    tmp = BannerStat(0, 0)
    tmp.add_click()
    assert tmp.clicks == 1
    tmp.add_click()
    assert tmp.clicks == 2


def test_get_banner_with_highest_cpc_returns_banner_with_highest_cpc(test_banners: list[Banner]) -> None:
    storage = BannerStorage(test_banners)
    ans = test_banners[0]
    for i in test_banners:
        if ans.cost * ans.stat.compute_ctr(0) < i.cost * i.stat.compute_ctr(0):
            ans = i
    assert ans == storage.banner_with_highest_cpc()

def test_banner_engine_raise_empty_storage_exception_if_constructed_with_empty_storage() -> None:
    storage = BannerStorage([])
    with pytest.raises(EmptyBannerStorageError):
        storage.banner_with_highest_cpc()

def test_engine_send_click_not_fails_on_unknown_banner(test_banners: list[Banner]) -> None:
    storage = BannerStorage(test_banners)
    engine = EpsilonGreedyBannerEngine(storage, 0.0)
    engine.send_click("non_existent_banner")


def test_engine_with_zero_random_probability_shows_banner_with_highest_cpc(test_banners: list[Banner]) -> None:
    storage = BannerStorage(test_banners)
    engine = EpsilonGreedyBannerEngine(storage, 0.0)
    best_banner = storage.banner_with_highest_cpc()
    shown_id = engine.show_banner()
    assert shown_id == best_banner.banner_id


@pytest.mark.parametrize("expected_random_banner", ["b1", "b2", "b3", "b4"])
def test_engine_with_1_random_banner_probability_gets_random_banner(
        expected_random_banner: str,
        test_banners: list[Banner],
        monkeypatch: typing.Any
        ) -> None:
        def mock_choice(lst):
            return expected_random_banner
        monkeypatch.setattr(random, "choice", mock_choice)
        storage = BannerStorage(test_banners)
        engine = EpsilonGreedyBannerEngine(storage, 1.0)
        shown_id = engine.show_banner()
        assert shown_id == expected_random_banner


def test_total_cost_equals_to_cost_of_clicked_banners(test_banners: list[Banner]) -> None:
    storage = BannerStorage(test_banners)
    engine = EpsilonGreedyBannerEngine(storage, 0.0)
    total_expected = 0
    for banner in test_banners:
        engine.show_banner()
        engine.send_click(banner.banner_id)
        total_expected += banner.cost
    assert engine.total_cost == total_expected


def test_engine_show_increases_banner_show_stat(test_banners: list[Banner]) -> None:
    storage = BannerStorage(test_banners)
    engine = EpsilonGreedyBannerEngine(storage, 0.0)
    initial_shows = test_banners[0].stat.shows
    engine.show_banner()
    # The banner that was shown should have +1 show
    best_banner = storage.banner_with_highest_cpc()
    assert best_banner.stat.shows == initial_shows + 1


def test_engine_click_increases_banner_click_stat(test_banners: list[Banner]) -> None:
    storage = BannerStorage(test_banners)
    engine = EpsilonGreedyBannerEngine(storage, 0.0)
    banner_id = test_banners[0].banner_id
    initial_clicks = test_banners[0].stat.clicks
    engine.send_click(banner_id)
    assert test_banners[0].stat.clicks == initial_clicks + 1
