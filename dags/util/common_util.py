from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from dto.tn_data_bsc_info import TnDataBscInfo
from dto.tn_data_clct_dtl_info import TnDataClctDtlInfo

logger = logging.getLogger(__name__)


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

    # ─────────────────────────────────────────────────────────────────────────
    # dags/detail 공통
    # - (1) 최초/증분: List DAG 가 원천 Car List 의 register_flag(Reg_Flag) 를 관리.
    #   Detail DAG 는 register_flag = 'A' 인 행만 상세 수집·Detail ODS 적재.
    # - (2) 재수집: 원천 Car List vs 원천 Detail ODS 비교 → Car List 의 complete_yn(Y/N) 갱신.
    #   Detail DAG 는 register_flag 를 수정하지 않는다.
    # ─────────────────────────────────────────────────────────────────────────

    # 최신 date_crtr_pnttm 스냅샷의 원천 Car List 전체(별칭 l)에 대해 Detail 존재 여부 반영
    DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT = "latest_snapshot"
    # date_crtr_pnttm 스냅샷 없이: register_flag <> N 이고 detail_url 있는 원천 List 행
    DETAIL_LIST_COMPLETE_FLAG_POLICY_NON_N_WITH_DETAIL_URL = "non_n_detail_url"

    @staticmethod
    def get_ods_table_columns(hook: Any, full_table_name: str) -> list[str]:
        schema, table = CommonUtil.split_schema_table(full_table_name)
        sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """
        rows = hook.get_records(sql, parameters=(schema, table))
        return [r[0] for r in rows]

    @staticmethod
    def build_detail_list_complete_flag_where_sql(
        list_table: str,
        policy: str,
        *,
        list_cols: set[str] | None = None,
    ) -> str:
        """UPDATE ... AS l 의 WHERE 절(별칭 l). list_cols 가 있으면 컬럼명을 DB 식별자(대소문자)에 맞춘다."""
        if policy == CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT:
            dcp = "date_crtr_pnttm"
            if list_cols is not None:
                dcp_m = CommonUtil.match_ods_column_name(list_cols, dcp)
                if not dcp_m:
                    raise ValueError(
                        f"list 테이블에 date_crtr_pnttm 컬럼을 찾을 수 없습니다: {list_table}"
                    )
                dcp = dcp_m
            return f"""
l."{dcp}" IS NOT NULL
AND l."{dcp}" = (
    SELECT MAX(m."{dcp}")
    FROM {list_table} m
    WHERE m."{dcp}" IS NOT NULL
)""".strip()
        if policy == CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_NON_N_WITH_DETAIL_URL:
            if list_cols is not None:
                reg = CommonUtil.match_ods_column_name(list_cols, "register_flag")
                det = CommonUtil.match_ods_column_name(list_cols, "detail_url")
                if not reg or not det:
                    raise ValueError(
                        f"list 테이블에 register_flag 또는 detail_url 을 찾을 수 없습니다: {list_table} "
                        f"(컬럼 예: {sorted(list_cols)[:50]})"
                    )
                return f"""
(l."{reg}" IS NULL OR TRIM(l."{reg}") <> 'N')
AND l."{det}" IS NOT NULL
AND TRIM(COALESCE(l."{det}"::text, '')) <> ''
""".strip()
            return """
(l.register_flag IS NULL OR TRIM(l.register_flag) <> 'N')
AND l.detail_url IS NOT NULL
AND TRIM(COALESCE(l.detail_url::text, '')) <> ''
""".strip()
        raise ValueError(f"지원하지 않는 complete_flag policy: {policy!r}")

    @staticmethod
    def match_ods_column_name(list_cols: set[str], logical_name: str) -> str | None:
        """
        information_schema 에서 온 컬럼명과 매칭. PostgreSQL 에서 따옴표로 만든 식별자는
        대소문자가 보존되므로 complete_yn 과 Complete_Yn 이 다르게 올 수 있다.
        """
        if logical_name in list_cols:
            return logical_name
        want = logical_name.lower()
        for c in list_cols:
            if c.lower() == want:
                return c
        return None

    @staticmethod
    def resolve_list_complete_column_name(list_cols: set[str], preferred: str) -> str | None:
        """
        원천 List 테이블에서 상세 수집 완료 여부를 넣을 컬럼명 확정.
        기본은 complete_yn (대소문자 변형 포함). 없으면 complete_flag 계열만 시도.
        """
        m = CommonUtil.match_ods_column_name(list_cols, preferred)
        if m:
            if m != preferred:
                logger.info(
                    "List 상세완료 컬럼: 선호 '%s' → 실제 식별자 '%s' 사용",
                    preferred,
                    m,
                )
            return m
        for alt in ("complete_yn", "complete_flag"):
            m = CommonUtil.match_ods_column_name(list_cols, alt)
            if m:
                logger.info(
                    "List 상세완료 컬럼: 선호 '%s' 없음 → '%s' 사용",
                    preferred,
                    m,
                )
                return m
        return None

    @staticmethod
    def bulk_insert_detail_ods_rows(
        hook: Any,
        full_table_name: str,
        rows: list[dict[str, Any]],
        *,
        truncate: bool = False,
        allow_only_table_cols: bool = True,
    ) -> None:
        """원천 Detail Car List(ODS) 적재. 테이블에 존재하는 컬럼만 INSERT."""
        if not rows:
            return

        table_cols = CommonUtil.get_ods_table_columns(hook, full_table_name) if allow_only_table_cols else []
        table_col_set = set(table_cols)

        candidate_cols: list[str] = []
        for row in rows:
            for key in row.keys():
                if key not in candidate_cols:
                    candidate_cols.append(key)

        insert_cols = [c for c in candidate_cols if c in table_col_set] if table_cols else candidate_cols
        if not insert_cols:
            raise ValueError(f"detail ODS insert 가능한 컬럼이 없습니다. table={full_table_name}")

        values = [tuple(row.get(col) for col in insert_cols) for row in rows]

        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                if truncate:
                    cur.execute(f"TRUNCATE TABLE {full_table_name}")

                from psycopg2.extras import execute_values

                cols_sql = ", ".join([f'"{col}"' for col in insert_cols])
                sql = f"INSERT INTO {full_table_name} ({cols_sql}) VALUES %s"
                execute_values(cur, sql, values, page_size=2000)
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def refresh_car_list_complete_flag_vs_detail_ods(
        hook: Any,
        *,
        list_table: str,
        detail_table: str,
        list_where_policy: str | None = None,
        list_where_sql: str | None = None,
        key_col: str = "product_id",
        list_complete_col: str = "complete_yn",
    ) -> int:
        """
        원천 Car List 와 원천 Detail ODS 를 key_col 로 매칭하여
        list_complete_col(기본 complete_yn) 에 Y(상세 ODS에 해당 키 행 존재) / N(미존재) 를 넣는다.
        register_flag(Reg_Flag) 는 변경하지 않는다.

        주의: Detail 적재가 증분(INSERT)이면 과거에 적재된 상세가 남아 있으면
        이번 실행에서 크롤이 실패해도 해당 차량은 Y 로 남을 수 있다.
        """
        list_cols = set(CommonUtil.get_ods_table_columns(hook, list_table))
        detail_cols = set(CommonUtil.get_ods_table_columns(hook, detail_table))

        if list_where_sql is None:
            if not list_where_policy:
                raise ValueError("list_where_policy 또는 list_where_sql 필요")
            list_where_sql = CommonUtil.build_detail_list_complete_flag_where_sql(
                list_table,
                list_where_policy,
                list_cols=list_cols,
            )

        col = CommonUtil.resolve_list_complete_column_name(list_cols, list_complete_col)
        if col is None:
            logger.warning(
                "상세완료 Y/N 갱신 스킵: list_complete_col='%s' 및 대체 후보를 테이블에서 찾지 못함 (%s)",
                list_complete_col,
                list_table,
            )
            return 0

        l_key = CommonUtil.match_ods_column_name(list_cols, key_col)
        d_key = CommonUtil.match_ods_column_name(detail_cols, key_col)
        if not l_key or not d_key:
            logger.error(
                "상세완료 Y/N 갱신 스킵: key_col='%s' 매칭 실패 — list→%s, detail→%s",
                key_col,
                l_key or "(해당 컬럼 없음)",
                d_key or "(해당 컬럼 없음)",
            )
            return 0

        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {list_table} AS l
                    SET "{col}" = CASE
                      WHEN EXISTS (
                        SELECT 1
                        FROM {detail_table} AS d
                        WHERE TRIM(COALESCE(d."{d_key}"::text, '')) <> ''
                          AND TRIM(COALESCE(d."{d_key}"::text, '')) =
                              TRIM(COALESCE(l."{l_key}"::text, ''))
                      ) THEN 'Y'
                      ELSE 'N'
                    END
                    WHERE {list_where_sql}
                    """,
                )
                n = int(cur.rowcount or 0)
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if n == 0:
            logger.warning(
                "상세완료 Y/N UPDATE 가 0건입니다. WHERE(list 정책)에 해당하는 행이 없거나, "
                "register_flag/detail_url 컬럼명이 테이블과 다를 수 있습니다. list=%s policy=%s",
                list_table,
                list_where_policy or "(custom sql)",
            )

        logger.info(
            "원천 Car List %s 갱신(Detail ODS 존재 여부): list=%s detail=%s rows=%d",
            col,
            list_table,
            detail_table,
            n,
        )
        return n

    @staticmethod
    def update_list_complete_yn_for_product_id(
        hook: Any,
        *,
        list_table: str,
        product_id: str,
        value: str,
        list_where_policy: str,
        list_complete_col: str = "complete_yn",
        register_flag_a_only: bool = False,
    ) -> int:
        """
        최신 List 스냅샷(WHERE policy) 안에서 product_id 가 일치하는 행의
        complete_yn(또는 resolve 된 동일 의미 컬럼)만 Y/N 으로 갱신한다.
        """
        list_cols = set(CommonUtil.get_ods_table_columns(hook, list_table))
        col = CommonUtil.resolve_list_complete_column_name(list_cols, list_complete_col)
        if col is None:
            logger.warning(
                "단건 complete_yn 갱신 스킵: list_complete_col='%s' 없음 (%s)",
                list_complete_col,
                list_table,
            )
            return 0
        l_key = CommonUtil.match_ods_column_name(list_cols, "product_id")
        if not l_key:
            logger.warning("단건 complete_yn 갱신 스킵: product_id 컬럼 없음 (%s)", list_table)
            return 0

        list_where_sql = CommonUtil.build_detail_list_complete_flag_where_sql(
            list_table,
            list_where_policy,
            list_cols=list_cols,
        )
        reg_extra = ""
        if register_flag_a_only:
            reg = CommonUtil.match_ods_column_name(list_cols, "register_flag")
            if reg:
                reg_extra = f""" AND TRIM(COALESCE(l."{reg}"::text, '')) = 'A' """

        pid = str(product_id or "").strip()
        if not pid:
            return 0

        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                sql = f"""
                    UPDATE {list_table} AS l
                    SET "{col}" = %s
                    WHERE {list_where_sql}
                      {reg_extra}
                      AND TRIM(COALESCE(l."{l_key}"::text, '')) = TRIM(COALESCE(%s::text, ''))
                    """
                # 호출부(DAG)에서 템플릿을 추측해 찍기보다, 실제 실행되는 최종 SQL을 여기서 로그로 남긴다.
                logger.info(
                    "단건 complete_yn 갱신 SQL ::: %s | params(value, product_id)=(%s, %s) | policy=%s register_flag_a_only=%s",
                    " ".join(sql.split()),
                    value,
                    pid,
                    list_where_policy,
                    register_flag_a_only,
                )
                cur.execute(sql, (value, pid))
                n = int(cur.rowcount or 0)
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if n == 0:
            logger.warning(
                "단건 complete_yn 갱신 0건: list=%s product_id=%s value=%s",
                list_table,
                pid,
                value,
            )
        return n
