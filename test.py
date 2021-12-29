from typing import List, Optional
from uuid import UUID, uuid4

from sqlalchemy import Column
from sqlalchemy_utils import UUIDType
from sqlmodel import Field, Relationship, Session, SQLModel, create_engine


class A(SQLModel, table=True):
    id: str = Field(primary_key=True)
    bs: List["B"] = Relationship(back_populates="a")


class B(SQLModel, table=True):
    id: int = Field(primary_key=True)
    a_uuid: str = Field(foreign_key="a.id")
    a: A = Relationship(back_populates="bs")


def main():
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    b1 = B(id=10)
    b2 = B(id=11)
    a = A(id=str(uuid4()), bs=[b1, b2])
    with Session(engine) as session:
        session.add(a)
        session.commit()
        session.refresh(a)
        print(a.bs)
        print(b1.a)


if __name__ == "__main__":
    main()
