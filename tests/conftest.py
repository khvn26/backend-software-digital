import pytest
from pytest_factoryboy import register

from tests import factories


@pytest.fixture(autouse=True)
def media_root(settings, tmp_path):
    path = tmp_path / "media"
    path.mkdir()
    settings.MEDIA_ROOT = path
    return path


@pytest.fixture
def dsr_files(settings):
    data_dir = settings.BASE_DIR / "data"
    return {
        # Compressed, have failing rows
        "Spotify_SpotifyStudent_SGAE_GB_GBP_20200101-20200430.tsv": data_dir
        / "Spotify_SpotifyStudent_GB_GBP_20200301-20200331.tsv.gz",
        "Spotify_SpotifyFree_SACEM_CH_CHF_20200201-20200228.tsv": data_dir
        / "Spotify_SpotifyFree_CH_CHF_20200201-20200228.tsv.gz",
        "Spotify_SpotifyFamilyPlan_SGAE_ES_EUR_20200101-20200331.tsv": data_dir
        / "Spotify_SpotifyFamilyPlan_ES_EUR_20200101-20200131.tsv.gz",
        "Spotify_SpotifyDuo_SGAE_NO_NOK_20200101-20200531.tsv": data_dir
        / "Spotify_SpotifyDuo_NO_NOK_20200101-20200131.tsv.gz",
        # Uncompressed, no failing rows
        "Spotify_SpotifyStudent_SGAE_GB_GBP_20210901-20210930.tsv": data_dir
        / "Spotify_SpotifyStudent_SGAE_GB_GBP_20210901-20210930.tsv",
    }


register(factories.TerritoryFactory, "territory")
register(factories.CurrencyFactory, "currency")
register(factories.DSRFactory, "dsr")
