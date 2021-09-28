import pytest

from dsrs.models import DSR, Resource

pytestmark = pytest.mark.django_db


def test_get_dsrs__return_expected(dsr, client):
    # act
    response = client.get(f"/dsrs/")

    # assert
    assert response.status_code == 200
    assert response.json() == [
        {
            "id": dsr.id,
            "path": dsr.path,
            "period_start": str(dsr.period_start),
            "period_end": str(dsr.period_end),
            "status": dsr.status,
            "territory": {"code_2": dsr.territory.code_2, "name": dsr.territory.name},
            "currency": {"code": dsr.currency.code, "name": dsr.currency.name},
        }
    ]


def test_get_dsrs_detail__return_expected(dsr, client):
    # act
    response = client.get(f"/dsrs/{dsr.id}/")

    # assert
    assert response.status_code == 200
    assert response.json() == {
        "id": dsr.id,
        "path": dsr.path,
        "period_start": str(dsr.period_start),
        "period_end": str(dsr.period_end),
        "status": dsr.status,
        "territory": {"code_2": dsr.territory.code_2, "name": dsr.territory.name},
        "currency": {"code": dsr.currency.code, "name": dsr.currency.name},
    }


def test_dsrs_import__empty_request__return_expected(client):
    # act
    response = client.post("/dsrs/import/")

    # assert
    assert response.status_code == 400


@pytest.mark.parametrize(
    "content",
    [
        b"foobar",
        b"\037\213",
        b"\037\213" * 10,
        b"\037\213\x08\x00\xde@\x03\x00\x00\x00",
        b"\x1f\x8b\x08\x08\xb5\xe3$`\x02\xffSpotif\000",
        b"\x1f\x8b\x08\x08\xb5\xe3$`\x02\xffSpotify_SpotifyDuo_SGAE_NO_NOK_20209999-20200531.tsv",
    ],
)
def test_dsrs_import__gzip__incorrect_content__return_expected(client, content):
    # act
    response = client.post("/dsrs/import/", content, content_type="*/*")

    # assert
    assert response.status_code == 400


def test_dsrs_import__gzip__empty_dsr__return_expected(client, mocker):
    # arrange
    content = b"\x1f\x8b\x08\x08\xb5\xe3$`\x02\xffSpotify_SpotifyDuo_SGAE_NO_NOK_20200101-20200531.tsv"

    # act
    response = client.post("/dsrs/import/", content, content_type="*/*")

    # assert
    assert response.status_code == 200

    response_json = response.json()
    assert response_json == {
        "id": mocker.ANY,
        "path": "Spotify_SpotifyDuo_SGAE_NO_NOK_20200101-20200531.tsv",
        "period_start": "2020-01-01",
        "period_end": "2020-05-31",
        "status": "ingested",
        "territory": {"code_2": "NO", "name": ""},
        "currency": {"code": "NOK", "name": ""},
    }

    dsr_id = response_json["id"]
    assert DSR.objects.get(id=dsr_id)
    assert not Resource.objects.filter(dsr_id=dsr_id)


@pytest.mark.parametrize(
    [
        "extra",
        "tsv_filename",
        "expected_status",
        "expected_resources_len",
        "expected_period_start",
        "expected_period_end",
        "expected_territory_code",
        "expected_currency_code",
    ],
    [
        (
            {},
            "Spotify_SpotifyStudent_SGAE_GB_GBP_20200101-20200430.tsv",
            "failed",
            81,
            "2020-01-01",
            "2020-04-30",
            "GB",
            "GBP",
        ),
        (
            {},
            "Spotify_SpotifyFree_SACEM_CH_CHF_20200201-20200228.tsv",
            "failed",
            80,
            "2020-02-01",
            "2020-02-28",
            "CH",
            "CHF",
        ),
        (
            {},
            "Spotify_SpotifyFamilyPlan_SGAE_ES_EUR_20200101-20200331.tsv",
            "failed",
            83,
            "2020-01-01",
            "2020-03-31",
            "ES",
            "EUR",
        ),
        (
            {},
            "Spotify_SpotifyDuo_SGAE_NO_NOK_20200101-20200531.tsv",
            "failed",
            86,
            "2020-01-01",
            "2020-05-31",
            "NO",
            "NOK",
        ),
        (
            {
                "HTTP_CONTENT_DISPOSITION": 'attachment; filename="Spotify_SpotifyStudent_SGAE_GB_GBP_20210901-20210930.tsv"'
            },
            "Spotify_SpotifyStudent_SGAE_GB_GBP_20210901-20210930.tsv",
            "ingested",
            13,
            "2021-09-01",
            "2021-09-30",
            "GB",
            "GBP",
        ),
    ],
)
def test_dsrs_import__populated_dsr__return_expected(
    extra,
    tsv_filename,
    expected_status,
    expected_resources_len,
    expected_period_start,
    expected_period_end,
    expected_territory_code,
    expected_currency_code,
    dsr_files,
    client,
    mocker,
    media_root,
):
    # arrange
    source_path = dsr_files[tsv_filename]
    content = open(source_path, mode="rb").read()

    # act
    response = client.post("/dsrs/import/", content, content_type="*/*", **extra)

    # assert
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json == {
        "id": mocker.ANY,
        "path": tsv_filename,
        "period_start": expected_period_start,
        "period_end": expected_period_end,
        "status": expected_status,
        "territory": {"code_2": expected_territory_code, "name": ""},
        "currency": {"code": expected_currency_code, "name": ""},
    }

    dsr_id = response_json["id"]
    assert DSR.objects.get(id=dsr_id)
    assert len(Resource.objects.filter(dsr_id=dsr_id)) == expected_resources_len
    assert (media_root / tsv_filename).exists()


@pytest.fixture
def ingested_dsrs(
    dsr_files,
    client,
):
    for tsv_filename, source_path in dsr_files.items():
        content = open(source_path, mode="rb").read()
        client.post(
            "/dsrs/import/",
            content,
            content_type="*/*",
            HTTP_CONTENT_DISPOSITION=f"attachment; filename={tsv_filename}",
        )
    return DSR.objects.all()


@pytest.mark.django_db(reset_sequences=True)
def test_resources_percentile__return_expected(
    ingested_dsrs,
    client,
):
    # act
    response = client.get(f"/resources/percentile/1/")

    # assert
    assert response.status_code == 200
    assert response.json() == [
        {
            "artists": "Mary Owens|Kyle Woods|Jessica Martin|Misty Reed",
            "dsp_id": "fzzUnrtVboqZVfWZjtvIDkgnUwtUqH",
            "dsr_ids": [1],
            "isrc": "GMUTT4545698",
            "revenue": "995649835527061.00000000000000000000",
            "title": "next trial will civil",
            "usages": 938305,
        },
        {
            "artists": "Steven Vincent|Christian Campbell",
            "dsp_id": "DcIQWUwJjFtNVJqgCJkXKRtKLrYzgb",
            "dsr_ids": [4],
            "isrc": "SBEJQ8975570",
            "revenue": "993340111782723.00000000000000000000",
            "title": "so business",
            "usages": 56883,
        },
        {
            "artists": "Walter Ramos",
            "dsp_id": "XBNthVJOlzOiXkaTyfYMFclYwpxQMT",
            "dsr_ids": [2],
            "isrc": "LBGZT0526482",
            "revenue": "948940145465677.00000000000000000000",
            "title": "theory carry true about",
            "usages": 887471,
        },
        {
            "artists": "Steven Thompson|Alvin Mcgee|Matthew Villanueva|Nathan "
            "Spencer|Denise Henderson MD",
            "dsp_id": "VHwVKwmLwnznBgmqlhNWXDwzvukIQE",
            "dsr_ids": [2],
            "isrc": "CRUNY3209492",
            "revenue": "921729463163152.00000000000000000000",
            "title": "conference customer plant few",
            "usages": 948437,
        },
    ]


@pytest.mark.django_db(reset_sequences=True)
def test_resources_percentile__params__return_expected(
    ingested_dsrs,
    client,
):
    # act
    response = client.get(
        f"/resources/percentile/5/?territory=NO&period_start=2020-02-01&period_end=2020-04-01"
    )

    # assert
    assert response.status_code == 200
    assert response.json() == [
        {
            "artists": "Walter Ramos",
            "dsp_id": "XBNthVJOlzOiXkaTyfYMFclYwpxQMT",
            "dsr_ids": [2],
            "isrc": "LBGZT0526482",
            "revenue": "948940145465677.00000000000000000000",
            "title": "theory carry true about",
            "usages": 887471,
        },
        {
            "artists": "Steven Thompson|Alvin Mcgee|Matthew Villanueva|Nathan "
            "Spencer|Denise Henderson MD",
            "dsp_id": "VHwVKwmLwnznBgmqlhNWXDwzvukIQE",
            "dsr_ids": [2],
            "isrc": "CRUNY3209492",
            "revenue": "921729463163152.00000000000000000000",
            "title": "conference customer plant few",
            "usages": 948437,
        },
        {
            "artists": "Mrs. Michelle Bowen|Zachary Ruiz",
            "dsp_id": "OwdHJNePxRccjjqpRsdRiQSiZoCJyP",
            "dsr_ids": [2],
            "isrc": "NAXYQ3435631",
            "revenue": "783864304943591.00000000000000000000",
            "title": "experience operation",
            "usages": 743598,
        },
        {
            "artists": "Mark Salazar|Daniel Chan|Anne Knapp|Lisa Woods|Tiffany Meyer",
            "dsp_id": "ZKHwCEepqoeTKYYeFqRhQStKYAnXGB",
            "dsr_ids": [2],
            "isrc": "KEASY4459734",
            "revenue": "647809913786876.00000000000000000000",
            "title": "who commercial increase",
            "usages": 935064,
        },
    ]
