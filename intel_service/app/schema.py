from pydantic import BaseModel
from datetime import  datetime
class Intel(BaseModel):
   timestamp: str
   signal_id: str
   entity_id: str
   reported_lat: float
   reported_lon: float
   signal_type: str
   priority_level: int

class Attack(BaseModel):
   timestamp: datetime
   attack_id: str
   entity_id: str
   weapon_type: str

class Damage(BaseModel):
   timestamp: datetime
   attack_id: str
   entity_id: str
   result: str


