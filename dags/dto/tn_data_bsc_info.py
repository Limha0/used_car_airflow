from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime

Base = declarative_base()

class TnDataBscInfo(Base):

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    __tablename__ = "tn_data_bsc_info"

    data_sn = Column(Integer)
    pvsn_site_cd = Column(String)
    datst_cd = Column(String, primary_key=True)
    datst_nm = Column(String)
    link_data_clct_url = Column(String)
    link_yn = Column(String)
    clct_yn = Column(String)
    ods_load_yn = Column(String)
    ods_load_mth_cd = Column(String)
    tmpr_tbl_phys_nm = Column(String)
    ods_tbl_phys_nm = Column(String)
    rmrk = Column(String)
    crt_id = Column(String)
    crt_dt = Column(DateTime)   
    mdfcn_id = Column(String)
    mdfcn_dt = Column(DateTime)