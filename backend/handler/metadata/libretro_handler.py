import hashlib
import os
import re
from typing import Final, NotRequired, TypedDict

from adapters.services.libretro_thumbnails import LibretroThumbnailsService
from adapters.services.libretro_thumbnails_types import LibretroArtType
from logger.logger import log

from .base_handler import MetadataHandler
from .base_handler import UniversalPlatformSlug as UPS

_PAREN_TAG_PATTERN = re.compile(r"\([^)]*\)")


class LibretroPlatform(TypedDict):
    slug: str
    libretro_slug: str | None


class LibretroRom(TypedDict):
    libretro_id: str | None
    url_cover: NotRequired[str]
    name: NotRequired[str]


def _remove_file_extension(filename: str) -> str:
    return os.path.splitext(filename)[0]


def _strip_paren_tags(s: str) -> str:
    """Remove parenthetical tags like (USA), (SGB Enhanced) from a filename."""
    return _PAREN_TAG_PATTERN.sub("", s).strip()


def libretro_id_for(filename: str) -> str:
    """Deterministic ID for a libretro art filename.

    SHA1 hex of the full filename (extension included). Stable across scans
    for the same matched art, fits in the `roms.libretro_id` column (40 chars
    in a varchar(64)).
    """
    return hashlib.sha1(filename.encode("utf-8"), usedforsecurity=False).hexdigest()


class LibretroHandler(MetadataHandler):
    """Handler for libretro thumbnails (https://thumbnails.libretro.com).

    Artwork-only source, supplies box-art URLs but no game IDs, summaries,
    or metadata. Follows the same integration pattern as SGDBBaseHandler.
    """

    def __init__(self) -> None:
        self.service = LibretroThumbnailsService()
        self.min_similarity_score: Final = 0.8

    @classmethod
    def is_enabled(cls) -> bool:
        return True

    async def heartbeat(self) -> bool:
        try:
            return await self.service.head()
        except Exception as exc:
            log.error("Error checking libretro thumbnails: %s", exc)
            return False

    def get_platform(self, slug: str) -> LibretroPlatform:
        if slug in LIBRETRO_PLATFORM_LIST:
            libretro_slug = LIBRETRO_PLATFORM_LIST[UPS(slug)]

            return LibretroPlatform(slug=slug, libretro_slug=libretro_slug)

        return LibretroPlatform(slug=slug, libretro_slug=None)

    def _find_exact_match(self, target: str, listing: list[str]) -> str | None:
        """Case-insensitive exact match on filename (extension stripped)."""
        target_lower = target.lower()
        for filename in listing:
            if _remove_file_extension(filename).lower() == target_lower:
                return filename
        return None

    def _find_fuzzy_match(self, target: str, listing: list[str]) -> str | None:
        """Fuzzy fallback, strips parenthetical tags from both sides and uses
        JaroWinkler via MetadataHandler.find_best_match."""
        if not listing:
            return None
        query = _strip_paren_tags(target)
        # Build candidate list of tag-stripped names that map back to original filenames
        stripped_to_original: dict[str, str] = {}
        for filename in listing:
            stripped = _strip_paren_tags(_remove_file_extension(filename))
            # Keep the first occurrence, libretro typically has one canonical
            # entry per region; ties are acceptable since we fall back here.
            stripped_to_original.setdefault(stripped, filename)

        match, _score = self.find_best_match(
            query,
            list(stripped_to_original.keys()),
            min_similarity_score=self.min_similarity_score,
        )
        if not match:
            return None
        return stripped_to_original[match]

    def _find_matching_art(self, fs_name: str, listing: list[str]) -> str | None:
        # Libretro's filename convention replaces '&' with '_'.
        cleaned = fs_name.replace("&", "_")
        target = _remove_file_extension(cleaned)

        exact = self._find_exact_match(target, listing)
        if exact:
            return exact

        return self._find_fuzzy_match(target, listing)

    async def get_rom(self, fs_name: str, platform_slug: str) -> LibretroRom:
        """Find box art for a ROM on the libretro thumbnail server.

        Scan-time callers use the returned `url_cover`. `name` is deliberately
        omitted because libretro artwork filenames are not proper game names —
        letting them overwrite a real name from IGDB/Moby would be wrong.
        """
        platform = self.get_platform(platform_slug)
        if not platform or not platform["libretro_slug"]:
            return LibretroRom(libretro_id=None)

        listing = await self.service.fetch_listing(
            platform["libretro_slug"], LibretroArtType.BOX_ART
        )
        if not listing:
            return LibretroRom(libretro_id=None)

        matched = self._find_matching_art(fs_name, listing)
        if not matched:
            return LibretroRom(libretro_id=None)

        url = LibretroThumbnailsService.build_art_url(
            platform["libretro_slug"], LibretroArtType.BOX_ART, matched
        )
        return LibretroRom(
            libretro_id=libretro_id_for(matched),
            url_cover=url,
        )


LIBRETRO_PLATFORM_LIST: Final[dict[UPS, str]] = {
    UPS.ADVENTURE_VISION: "Entex - Adventure Vision",
    UPS.AMIGA: "Commodore - Amiga",
    UPS.AMIGA_CD32: "Commodore - Amiga",
    UPS.ACPC: "Amstrad - CPC",
    UPS.ATARI2600: "Atari - 2600",
    UPS.ATARI5200: "Atari - 5200",
    UPS.ATARI7800: "Atari - 7800",
    UPS.ATARI_ST: "Atari - ST",
    UPS.JAGUAR: "Atari - Jaguar",
    UPS.LYNX: "Atari - Lynx",
    UPS.WONDERSWAN: "Bandai - WonderSwan",
    UPS.WONDERSWAN_COLOR: "Bandai - WonderSwan Color",
    UPS.COLECOVISION: "Coleco - ColecoVision",
    UPS.C64: "Commodore - 64",
    UPS.VIC_20: "Commodore - VIC-20",
    UPS.DOS: "DOS",
    UPS.FAIRCHILD_CHANNEL_F: "Fairchild - Channel F",
    UPS.VECTREX: "GCE - Vectrex",
    UPS.ODYSSEY_2: "Magnavox - Odyssey2",
    UPS.INTELLIVISION: "Mattel - Intellivision",
    UPS.MSX: "Microsoft - MSX",
    UPS.MSX2: "Microsoft - MSX2",
    UPS.XBOX: "Microsoft - XBOX",
    UPS.PC_8800_SERIES: "NEC - PC Engine - TurboGrafx 16",
    UPS.PC_FX: "NEC - PC-FX",
    UPS.PC_9800_SERIES: "NEC - PC-98",
    UPS.SUPERGRAFX: "NEC - PC Engine SuperGrafx",
    UPS.TG16: "NEC - PC Engine - TurboGrafx 16",
    UPS.TURBOGRAFX_CD: "NEC - PC Engine CD - TurboGrafx-CD",
    UPS.FDS: "Nintendo - Family Computer Disk System",
    UPS.GB: "Nintendo - Game Boy",
    UPS.GBA: "Nintendo - Game Boy Advance",
    UPS.GBC: "Nintendo - Game Boy Color",
    UPS.NGC: "Nintendo - GameCube",
    UPS.N64: "Nintendo - Nintendo 64",
    UPS.N64DD: "Nintendo - Nintendo 64DD",
    UPS.N3DS: "Nintendo - Nintendo 3DS",
    UPS.NDS: "Nintendo - Nintendo DS",
    UPS.NES: "Nintendo - Nintendo Entertainment System",
    UPS.FAMICOM: "Nintendo - Nintendo Entertainment System",
    UPS.POKEMON_MINI: "Nintendo - Pokemon Mini",
    UPS.SATELLAVIEW: "Nintendo - Satellaview",
    UPS.SUFAMI_TURBO: "Nintendo - Sufami Turbo",
    UPS.SNES: "Nintendo - Super Nintendo Entertainment System",
    UPS.SFAM: "Nintendo - Super Nintendo Entertainment System",
    UPS.VIRTUALBOY: "Nintendo - Virtual Boy",
    UPS.WII: "Nintendo - Wii",
    UPS.WIIU: "Nintendo - Wii U",
    UPS.SCUMMVM: "ScummVM",
    UPS.SEGA32: "Sega - 32X",
    UPS.DC: "Sega - Dreamcast",
    UPS.GAMEGEAR: "Sega - Game Gear",
    UPS.GENESIS: "Sega - Mega Drive - Genesis",
    UPS.SEGACD: "Sega - Mega-CD - Sega CD",
    UPS.SMS: "Sega - Master System - Mark III",
    UPS.SG1000: "Sega - SG-1000",
    UPS.SATURN: "Sega - Saturn",
    UPS.X1: "Sharp - X1",
    UPS.SHARP_X68000: "Sharp - X68000",
    UPS.ZX81: "Sinclair - ZX 81",
    UPS.ZXS: "Sinclair - ZX Spectrum",
    UPS.NEOGEOAES: "SNK - Neo Geo",
    UPS.NEOGEOMVS: "SNK - Neo Geo",
    UPS.NEO_GEO_CD: "SNK - Neo Geo CD",
    UPS.NEO_GEO_POCKET: "SNK - Neo Geo Pocket",
    UPS.NEO_GEO_POCKET_COLOR: "SNK - Neo Geo Pocket Color",
    UPS.PSX: "Sony - PlayStation",
    UPS.PS2: "Sony - PlayStation 2",
    UPS.PSP: "Sony - PlayStation Portable",
    UPS.TIC_80: "TIC-80",
    UPS.TOMY_TUTOR: "Tomy - Tutor",
    UPS.SUPERVISION: "Watara - Supervision",
}
