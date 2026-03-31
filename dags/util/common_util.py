from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from dto.tn_data_bsc_info import TnDataBscInfo
from dto.tn_data_clct_dtl_info import TnDataClctDtlInfo


class CommonUtil:
    @staticmethod
    def serialize_row(row: dict[str, Any]) -> dict[str, Any]:
        serialized: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            else:
                serialized[key] = value
        return serialized

    @staticmethod
    def build_bsc_info_dto(row: dict[str, Any]) -> TnDataBscInfo:
        dto = TnDataBscInfo()
        serialized = CommonUtil.serialize_row(row)
        for column in TnDataBscInfo.__table__.columns:
            value = serialized.get(column.name)
            if value is not None:
                setattr(dto, column.name, value)
        if getattr(dto, "link_data_clct_url", None) is None and serialized.get("link_url") is not None:
            dto.link_data_clct_url = serialized.get("link_url")
        if getattr(dto, "ods_tbl_phys_nm", None) is None:
            target_table = serialized.get("ods_tbl_phys_nm") or serialized.get("target_table") or serialized.get("trgt_tbl_nm") or serialized.get("target_tbl")
            if target_table is not None:
                dto.ods_tbl_phys_nm = target_table
        if getattr(dto, "tmpr_tbl_phys_nm", None) is None:
            temp_table = serialized.get("tmpr_tbl_phys_nm") or serialized.get("tmp_tbl_phys_nm") or serialized.get("temp_tbl_phys_nm")
            if temp_table is not None:
                dto.tmpr_tbl_phys_nm = temp_table
        return dto

    @staticmethod
    def bsc_info_to_dict(dto: TnDataBscInfo) -> dict[str, Any]:
        return dto.as_dict()

    @staticmethod
    def build_collect_detail_dto(row: dict[str, Any]) -> TnDataClctDtlInfo:
        dto = TnDataClctDtlInfo()
        for column in TnDataClctDtlInfo.__table__.columns:
            value = row.get(column.name)
            if value is not None:
                setattr(dto, column.name, value)
        return dto

    @staticmethod
    def build_dated_site_path(
        root_path: str | Path,
        site_name: str,
        dt: datetime | None = None,
    ) -> Path:
        now = dt or datetime.now()
        year_str = now.strftime("%Y년")
        date_str = now.strftime("%Y%m%d")
        return Path(root_path) / year_str / site_name / date_str

    @staticmethod
    def build_year_site_path(
        root_path: str | Path,
        site_name: str,
        dt: datetime | None = None,
    ) -> Path:
        """
        이미지 저장처럼 날짜 폴더 없이 연도/사이트까지만 필요한 경우 사용.
        예) {root}/2026년/롯데렌터카
        """
        now = dt or datetime.now()
        year_str = now.strftime("%Y년")
        return Path(root_path) / year_str / site_name

    @staticmethod
    def clear_image_files(
        dir_path: str | Path,
        *,
        recursive: bool = False,
        extensions: set[str] | None = None,
    ) -> int:
        """
        디렉터리 내부의 '이미지 파일'만 삭제한다(디렉터리/비이미지 파일은 유지).

        - **dir_path**: 대상 디렉터리
        - **recursive**: True면 하위 디렉터리까지 순회(rglob)
        - **extensions**: 삭제 대상 확장자 집합(점 포함, 소문자). None이면 기본 이미지 확장자 사용

        반환값: 삭제된 파일 개수
        """
        path = Path(dir_path)
        if not path.exists() or not path.is_dir():
            return 0

        exts = extensions or {
            ".png",
            ".jpg",
            ".jpeg",
            ".webp",
            ".gif",
            ".bmp",
        }
        exts = {e.lower() if e.startswith(".") else ("." + e.lower()) for e in exts}

        deleted = 0
        it = path.rglob("*") if recursive else path.iterdir()
        for child in it:
            try:
                if not child.is_file():
                    continue
                if child.suffix.lower() not in exts:
                    continue
                child.unlink()
                deleted += 1
            except Exception:
                continue
        return deleted

    @staticmethod
    def split_schema_table(full_table_name: str) -> tuple[str, str]:
        if "." in full_table_name:
            schema, table = full_table_name.split(".", 1)
            return schema.strip(), table.strip()
        return "public", full_table_name.strip()

    @staticmethod
    def extract_run_ts_from_csv_path(csv_path: Path) -> str:
        stem = csv_path.stem
        if "_" not in stem:
            return ""
        run_ts = stem.rsplit("_", 1)[-1]
        return run_ts if run_ts.isdigit() else ""

    @staticmethod
    def upsert_collect_detail_info(
        hook: Any,
        detail_table: str,
        datst_cd: str,
        csv_path: Path,
    ) -> TnDataClctDtlInfo:
        file_name = csv_path.name
        file_path = str(csv_path.parent)
        extn = csv_path.suffix.lstrip(".")
        clct_pnttm = CommonUtil.extract_run_ts_from_csv_path(csv_path) or datetime.now().strftime("%Y%m%d%H%M")

        select_sql = f"""
        SELECT dtl_info_sn, datst_cd, clct_data_file_nm, clct_flpth, extn, clct_pnttm
        FROM {detail_table}
        WHERE datst_cd = %s
          AND clct_data_file_nm = %s
          AND COALESCE(clct_flpth, '') = %s
          AND COALESCE(clct_pnttm, '') = %s
        ORDER BY dtl_info_sn DESC
        LIMIT 1
        """
        rows = hook.get_records(select_sql, parameters=(datst_cd, file_name, file_path, clct_pnttm))
        if rows:
            dtl_info_sn, _, clct_data_file_nm, clct_flpth, extn_val, clct_pnttm_val = rows[0]
            dto = CommonUtil.build_collect_detail_dto({
                "dtl_info_sn": dtl_info_sn,
                "datst_cd": datst_cd,
                "clct_data_file_nm": clct_data_file_nm,
                "clct_flpth": clct_flpth or "",
                "extn": extn_val or "",
                "clct_pnttm": clct_pnttm_val or "",
            })
            setattr(dto, "status", "existing")
            return dto

        insert_sql = f"""
        INSERT INTO {detail_table} (
            datst_cd,
            clct_data_file_nm,
            clct_flpth,
            extn,
            clct_pnttm
        ) VALUES (%s, %s, %s, %s, %s)
        RETURNING dtl_info_sn
        """
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(insert_sql, (datst_cd, file_name, file_path, extn, clct_pnttm))
                dtl_info_sn = cur.fetchone()[0]
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        dto = CommonUtil.build_collect_detail_dto({
            "dtl_info_sn": dtl_info_sn,
            "datst_cd": datst_cd,
            "clct_data_file_nm": file_name,
            "clct_flpth": file_path,
            "extn": extn,
            "clct_pnttm": clct_pnttm,
        })
        setattr(dto, "status", "inserted")
        return dto

    @staticmethod
    def get_latest_collect_detail_info(
        hook: Any,
        detail_table: str,
        datst_cd: str,
    ) -> TnDataClctDtlInfo | None:
        sql = f"""
        SELECT dtl_info_sn, datst_cd, clct_data_file_nm, clct_flpth, extn, clct_pnttm
        FROM {detail_table}
        WHERE datst_cd = %s
        ORDER BY clct_pnttm DESC NULLS LAST, dtl_info_sn DESC
        LIMIT 1
        """
        rows = hook.get_records(sql, parameters=(datst_cd,))
        if not rows:
            return None

        dtl_info_sn, datst_cd_val, clct_data_file_nm, clct_flpth, extn, clct_pnttm = rows[0]
        return CommonUtil.build_collect_detail_dto({
            "dtl_info_sn": dtl_info_sn,
            "datst_cd": datst_cd_val,
            "clct_data_file_nm": clct_data_file_nm or "",
            "clct_flpth": clct_flpth or "",
            "extn": extn or "",
            "clct_pnttm": clct_pnttm or "",
        })

    @staticmethod
    def build_collect_detail_file_path(row: TnDataClctDtlInfo | dict[str, Any]) -> Path:
        dto = row if isinstance(row, TnDataClctDtlInfo) else CommonUtil.build_collect_detail_dto(row)
        file_name = "" if dto.clct_data_file_nm is None else str(dto.clct_data_file_nm).strip()
        file_path = "" if dto.clct_flpth is None else str(dto.clct_flpth).strip()
        if not file_name:
            return Path(file_path)
        if file_path:
            return Path(file_path) / file_name
        return Path(file_name)

    @staticmethod
    def get_table_row_count(
        hook: Any,
        full_table_name: str,
    ) -> int:
        schema, table = CommonUtil.split_schema_table(full_table_name)
        sql = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        """
        exists_rows = hook.get_records(sql, parameters=(schema, table))
        if not exists_rows or int(exists_rows[0][0]) == 0:
            raise ValueError(f"count 대상 테이블이 없습니다: {full_table_name}")

        count_rows = hook.get_records(f"SELECT COUNT(*) FROM {full_table_name}")
        if not count_rows:
            return 0
        return int(count_rows[0][0] or 0)
