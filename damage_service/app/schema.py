from pydantic import BaseModel

class Damage(BaseModel):
   timestamp: str
   attack_id: str
   entity_id: str
   result: str