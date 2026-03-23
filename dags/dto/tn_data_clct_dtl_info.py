from sqlalchemy import BigInteger, Column, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class TnDataClctDtlInfo(Base):

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    __tablename__ = "tn_data_clct_dtl_info"

    dtl_info_sn = Column(BigInteger, primary_key=True, autoincrement=True)
    datst_cd = Column(String, nullable=False)
    clct_data_file_nm = Column(String, nullable=False)
    clct_flpth = Column(String)
    extn = Column(String)
    clct_pnttm = Column(String)