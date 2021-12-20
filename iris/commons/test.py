from sqlmodel import SQLModel, create_engine


def create_test_engine():
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    return engine
