from typing import TypedDict
from unittest.mock import MagicMock

import pytest

from handler.database import db_platform_handler, db_rom_handler
from handler.filesystem import fs_resource_handler
from models.platform import Platform
from models.rom import Rom, RomMetadata
from models.user import User
from utils.pegasus_exporter import PegasusExporter


class ParsedPegasus(TypedDict):
    collection: dict[str, str]
    games: list[dict[str, str | list[str]]]


def _mock_rom(**overrides) -> Rom:
    """Create a MagicMock with spec=Rom and sensible defaults for exporter tests."""
    defaults = {
        "id": 1,
        "name": None,
        "fs_name": "rom.bin",
        "fs_name_no_tags": "rom",
        "fs_name_no_ext": "rom",
        "fs_resources_path": "roms/1/1",
        "summary": None,
        "regions": None,
        "languages": None,
        "tags": None,
        "ss_metadata": None,
        "gamelist_metadata": None,
        "path_cover_l": None,
        "path_screenshots": None,
        "path_video": None,
        "metadatum": None,
    }
    defaults.update(overrides)
    mock = MagicMock(spec=Rom)
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


@pytest.fixture
def platform_with_roms(admin_user: User):
    platform = Platform(name="Super Nintendo", slug="snes", fs_slug="snes")
    platform = db_platform_handler.add_platform(platform)

    rom = Rom(
        platform_id=platform.id,
        name="Super Mario World",
        slug="super-mario-world",
        fs_name="Super Mario World (USA).sfc",
        fs_name_no_tags="Super Mario World",
        fs_name_no_ext="Super Mario World (USA)",
        fs_extension="sfc",
        fs_path="snes/roms",
        summary="A classic platformer game.",
        regions=["USA"],
        languages=["en"],
        tags=["Retro", "Classic"],
        gamelist_id="12345",
        gamelist_metadata={"player_count": "2"},
    )
    rom = db_rom_handler.add_rom(rom)
    db_rom_handler.add_rom_user(rom_id=rom.id, user_id=admin_user.id)

    metadata = RomMetadata(
        rom_id=rom.id,
        genres=["Platformer", "Adventure"],
        companies=["Nintendo", "Nintendo EAD"],
        first_release_date=709344000000,  # 1992-06-23
        average_rating=9.2,
    )
    from tests.conftest import session as test_session

    with test_session.begin() as s:
        s.merge(metadata)

    rom = db_rom_handler.get_rom(rom.id)
    return platform, [rom]


@pytest.fixture
def platform_with_minimal_rom(admin_user: User):
    platform = Platform(name="Game Boy", slug="gb", fs_slug="gb")
    platform = db_platform_handler.add_platform(platform)

    rom = Rom(
        platform_id=platform.id,
        name=None,
        slug="unknown-rom",
        fs_name="unknown.gb",
        fs_name_no_tags="unknown",
        fs_name_no_ext="unknown",
        fs_extension="gb",
        fs_path="gb/roms",
    )
    rom = db_rom_handler.add_rom(rom)
    db_rom_handler.add_rom_user(rom_id=rom.id, user_id=admin_user.id)

    return platform, [rom]


def _parse_pegasus(content: str) -> ParsedPegasus:
    """Parse Pegasus metadata content into a structured dict."""
    result: ParsedPegasus = {"collection": {}, "games": []}
    current_game: dict[str, str | list[str]] | None = None
    current_key = None

    for line in content.splitlines():
        if not line.strip():
            if current_game:
                result["games"].append(current_game)
                current_game = None
                current_key = None
            continue

        if line.startswith("  "):
            # Continuation line for multiline value
            if current_game and current_key:
                val = line.strip()
                if val == ".":
                    val = ""
                current_game[current_key] += "\n" + val
            continue

        if ":" not in line:
            continue

        key, _, value = line.partition(": ")
        value = value.strip()

        if key == "collection":
            result["collection"]["name"] = value
        elif key == "shortname":
            result["collection"]["shortname"] = value
        elif key == "game":
            if current_game:
                result["games"].append(current_game)
            current_game = {"game": value}
            current_key = None
        elif current_game is not None:
            if key in current_game:
                # Multi-value field (e.g., genre, tag)
                existing_value = current_game[key]
                if isinstance(existing_value, list):
                    existing_value.append(value)
                else:
                    current_game[key] = [existing_value, value]
            else:
                current_game[key] = value
            current_key = key

    if current_game:
        result["games"].append(current_game)

    return result


def test_export_pegasus_collection_header(platform_with_roms):
    platform, _ = platform_with_roms
    exporter = PegasusExporter(local_export=True)

    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)

    assert parsed["collection"]["name"] == "Super Nintendo"
    assert parsed["collection"]["shortname"] == "snes"


def test_export_pegasus_game_entry(platform_with_roms):
    platform, _ = platform_with_roms
    exporter = PegasusExporter(local_export=True)

    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)

    assert len(parsed["games"]) == 1
    game = parsed["games"][0]

    assert game["game"] == "Super Mario World"
    assert game["file"] == "Super Mario World (USA).sfc"
    assert game["developer"] == "Nintendo"
    assert game["publisher"] == "Nintendo EAD"
    assert game["description"] == "A classic platformer game."


def test_export_pegasus_sort_by(platform_with_roms):
    platform, _ = platform_with_roms
    exporter = PegasusExporter(local_export=True)

    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)
    game = parsed["games"][0]

    # name="Super Mario World", fs_name_no_tags="Super Mario World" — same, so no sort-by
    assert "sort-by" not in game


def test_export_pegasus_genres(platform_with_roms):
    platform, _ = platform_with_roms
    exporter = PegasusExporter(local_export=True)

    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)
    game = parsed["games"][0]

    assert game["genre"] == ["Platformer", "Adventure"]


def test_export_pegasus_tags(platform_with_roms):
    platform, _ = platform_with_roms
    exporter = PegasusExporter(local_export=True)

    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)
    game = parsed["games"][0]

    assert game["tag"] == ["Retro", "Classic"]


def test_export_pegasus_rating(platform_with_roms):
    platform, _ = platform_with_roms
    exporter = PegasusExporter(local_export=True)

    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)
    game = parsed["games"][0]

    # 9.2 * 10 = 92%
    assert game["rating"] == "92%"


def test_export_pegasus_release_date(platform_with_roms):
    platform, _ = platform_with_roms
    exporter = PegasusExporter(local_export=True)

    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)
    game = parsed["games"][0]

    assert isinstance(game["release"], str)
    assert game["release"].startswith("1992-")


def test_export_pegasus_extension_fields(platform_with_roms):
    platform, _ = platform_with_roms
    exporter = PegasusExporter(local_export=True)

    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)
    game = parsed["games"][0]

    assert game["x-region"] == "USA"
    assert game["x-language"] == "en"
    assert "x-romm-id" in game


def test_export_pegasus_minimal_rom(platform_with_minimal_rom):
    platform, _ = platform_with_minimal_rom
    exporter = PegasusExporter(local_export=True)

    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)

    assert len(parsed["games"]) == 1
    game = parsed["games"][0]

    # Falls back to fs_name when name is None
    assert game["game"] == "unknown.gb"
    assert game["file"] == "unknown.gb"
    assert "developer" not in game
    assert "genre" not in game
    assert "description" not in game


def test_export_pegasus_skips_missing_roms(admin_user: User):
    platform = Platform(name="NES", slug="nes", fs_slug="nes")
    platform = db_platform_handler.add_platform(platform)

    rom = Rom(
        platform_id=platform.id,
        name="Missing ROM",
        slug="missing-rom",
        fs_name="missing.nes",
        fs_name_no_tags="missing",
        fs_name_no_ext="missing",
        fs_extension="nes",
        fs_path="nes/roms",
        missing_from_fs=True,
    )
    db_rom_handler.add_rom(rom)

    exporter = PegasusExporter(local_export=True)
    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)

    assert len(parsed["games"]) == 0


def test_export_pegasus_skips_metadata_files(admin_user: User):
    platform = Platform(name="Genesis", slug="genesis", fs_slug="genesis")
    platform = db_platform_handler.add_platform(platform)

    for fname, ext in [("gamelist.xml", "xml"), ("metadata.pegasus.txt", "txt")]:
        rom = Rom(
            platform_id=platform.id,
            name=fname,
            slug=fname,
            fs_name=fname,
            fs_name_no_tags=fname.split(".")[0],
            fs_name_no_ext=fname.rsplit(".", 1)[0],
            fs_extension=ext,
            fs_path="genesis/roms",
        )
        db_rom_handler.add_rom(rom)

    exporter = PegasusExporter(local_export=True)
    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)

    assert len(parsed["games"]) == 0


def test_export_pegasus_invalid_platform():
    exporter = PegasusExporter(local_export=True)

    with pytest.raises(ValueError, match="not found"):
        exporter.export_platform_to_pegasus(99999, request=None)


def test_export_pegasus_multiline_description(admin_user: User):
    platform = Platform(name="GBA", slug="gba", fs_slug="gba")
    platform = db_platform_handler.add_platform(platform)

    rom = Rom(
        platform_id=platform.id,
        name="Test Game",
        slug="test-game",
        fs_name="test.gba",
        fs_name_no_tags="test",
        fs_name_no_ext="test",
        fs_extension="gba",
        fs_path="gba/roms",
        summary="First line.\n\nThird line after blank.",
    )
    rom = db_rom_handler.add_rom(rom)
    db_rom_handler.add_rom_user(rom_id=rom.id, user_id=admin_user.id)

    exporter = PegasusExporter(local_export=True)
    content = exporter.export_platform_to_pegasus(platform.id, request=None)

    # Verify the raw output has proper indentation for multiline
    assert "description: First line." in content
    assert "  ." in content  # blank line becomes "  ."
    assert "  Third line after blank." in content


def test_export_pegasus_custom_platform_name(admin_user: User):
    platform = Platform(
        name="Super Nintendo",
        slug="snes",
        fs_slug="snes",
        custom_name="SNES (Custom)",
    )
    platform = db_platform_handler.add_platform(platform)

    exporter = PegasusExporter(local_export=True)
    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)

    assert parsed["collection"]["name"] == "SNES (Custom)"


def test_export_pegasus_multiple_roms(admin_user: User):
    platform = Platform(name="N64", slug="n64", fs_slug="n64")
    platform = db_platform_handler.add_platform(platform)

    for i in range(3):
        rom = Rom(
            platform_id=platform.id,
            name=f"Game {i}",
            slug=f"game-{i}",
            fs_name=f"game{i}.n64",
            fs_name_no_tags=f"game{i}",
            fs_name_no_ext=f"game{i}",
            fs_extension="n64",
            fs_path="n64/roms",
        )
        rom = db_rom_handler.add_rom(rom)
        db_rom_handler.add_rom_user(rom_id=rom.id, user_id=admin_user.id)

    exporter = PegasusExporter(local_export=True)
    content = exporter.export_platform_to_pegasus(platform.id, request=None)
    parsed = _parse_pegasus(content)

    assert len(parsed["games"]) == 3


def test_format_rating():
    exporter = PegasusExporter()
    assert exporter._format_rating(10.0) == "100%"
    assert exporter._format_rating(0.0) == "0%"
    assert exporter._format_rating(7.5) == "75%"


def test_escape_multiline():
    exporter = PegasusExporter()

    assert exporter._escape_multiline("single line") == "single line"
    assert exporter._escape_multiline("line1\nline2") == "line1\n  line2"
    assert exporter._escape_multiline("line1\n\nline3") == "line1\n  .\n  line3"


def test_game_media_dir_name():
    exporter = PegasusExporter()
    rom = _mock_rom(fs_name_no_ext="Super Mario World (USA)")
    assert exporter._game_media_dir_name(rom) == "Super Mario World (USA)"


def test_collect_assets_empty():
    """When no resource paths are set, _collect_assets returns empty dict."""
    exporter = PegasusExporter(local_export=True)
    rom = _mock_rom(
        path_cover_l=None,
        path_screenshots=None,
        path_video=None,
        ss_metadata=None,
        gamelist_metadata=None,
    )
    assert exporter._collect_assets(rom) == {}


def test_collect_assets_with_cover(tmp_path, monkeypatch):
    """Cover is picked up from rom.path_cover_l and mapped to box_front."""
    monkeypatch.setattr(fs_resource_handler, "base_path", tmp_path)
    exporter = PegasusExporter(local_export=True)

    cover_file = tmp_path / "roms" / "1" / "1" / "cover" / "big.png"
    cover_file.parent.mkdir(parents=True)
    cover_file.write_bytes(b"fake png")

    rom = _mock_rom(
        path_cover_l="roms/1/1/cover/big.png",
        path_screenshots=None,
        path_video=None,
        ss_metadata=None,
        gamelist_metadata=None,
    )
    assets = exporter._collect_assets(rom)
    assert "box_front" in assets
    assert assets["box_front"] == cover_file


def test_collect_assets_with_screenshots(tmp_path, monkeypatch):
    """First screenshot is picked up from rom.path_screenshots."""
    monkeypatch.setattr(fs_resource_handler, "base_path", tmp_path)
    exporter = PegasusExporter(local_export=True)

    ss_file = tmp_path / "roms" / "1" / "1" / "screenshots" / "0.jpg"
    ss_file.parent.mkdir(parents=True)
    ss_file.write_bytes(b"fake jpg")

    rom = _mock_rom(
        path_cover_l=None,
        path_screenshots=["roms/1/1/screenshots/0.jpg"],
        path_video=None,
        ss_metadata=None,
        gamelist_metadata=None,
    )
    assets = exporter._collect_assets(rom)
    assert "screenshot" in assets
    assert assets["screenshot"] == ss_file


def test_collect_assets_with_video(tmp_path, monkeypatch):
    """Video is picked up from rom.path_video."""
    monkeypatch.setattr(fs_resource_handler, "base_path", tmp_path)
    exporter = PegasusExporter(local_export=True)

    video_file = tmp_path / "roms" / "1" / "1" / "video" / "video.mp4"
    video_file.parent.mkdir(parents=True)
    video_file.write_bytes(b"fake mp4")

    rom = _mock_rom(
        path_cover_l=None,
        path_screenshots=None,
        path_video="roms/1/1/video/video.mp4",
        ss_metadata=None,
        gamelist_metadata=None,
    )
    assets = exporter._collect_assets(rom)
    assert "video" in assets
    assert assets["video"] == video_file


def test_collect_assets_ss_metadata(tmp_path, monkeypatch):
    """Extended media from ss_metadata is picked up."""
    monkeypatch.setattr(fs_resource_handler, "base_path", tmp_path)
    exporter = PegasusExporter(local_export=True)

    box3d_file = tmp_path / "roms" / "1" / "1" / "box3d" / "box3d.png"
    box3d_file.parent.mkdir(parents=True)
    box3d_file.write_bytes(b"fake")

    rom = _mock_rom(
        path_cover_l=None,
        path_screenshots=None,
        path_video=None,
        ss_metadata={"box3d_path": "roms/1/1/box3d/box3d.png"},
        gamelist_metadata=None,
    )
    assets = exporter._collect_assets(rom)
    assert "box_full" in assets
    assert assets["box_full"] == box3d_file


def test_create_game_entry_with_assets():
    """Asset references are included in the game entry."""
    exporter = PegasusExporter(local_export=True)

    metadatum = MagicMock()
    metadatum.companies = None
    metadatum.genres = None
    metadatum.player_count = None
    metadatum.first_release_date = None
    metadatum.average_rating = None

    rom = _mock_rom(
        name="Test Game",
        fs_name="test.sfc",
        fs_name_no_tags="test",
        metadatum=metadatum,
    )

    exported_assets = {
        "box_front": "assets/covers/test.png",
        "screenshot": "assets/screenshots/test.jpg",
    }

    entry = exporter._create_game_entry(
        rom, request=None, exported_assets=exported_assets
    )
    assert "assets.box_front: assets/covers/test.png" in entry
    assert "assets.screenshot: assets/screenshots/test.jpg" in entry
