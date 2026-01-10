from dataclasses import dataclass

@dataclass
class User:
    id: int
    name: str
    age: int
    balance: str

USER_DATA = [
    User(id=1, name="John", age=30, balance="100.50"),
    User(id=2, name="Jane", age=25, balance="250.75"),
    User(id=3, name="Joe", age=78, balance="0.0")
]