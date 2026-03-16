from pydantic import BaseModel
from datetime import  datetime

class Attack(BaseModel):
   timestamp: datetime
   attack_id: str
   entity_id: str
   weapon_type: str
