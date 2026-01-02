from dataclasses import dataclass

@dataclass
class User:
    id: str
    name: str
    age: str

USER_DATA = [
    User("1", "John", "30"),
    User("2", "Jane", "25"),
    User("3", "Joe", "20")
    ]
