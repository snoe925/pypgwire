from dataclasses import dataclass
from decimal import Decimal

@dataclass
class User:
    id: int
    name: str
    age: int
    balance: float
    # Example column backed by Postgres NUMERIC (OID 1700) and Python Decimal.
    # We keep it as Decimal in Python to avoid float rounding issues.
    interest: Decimal

USER_DATA = [
    User(id=1, name="John", age=30, balance=100.50, interest=Decimal('3.50')),
    User(id=2, name="Jane", age=25, balance=250.75, interest=Decimal('4.125')),
    User(id=3, name="Joe", age=78, balance=0.0, interest=Decimal('0'))
]
