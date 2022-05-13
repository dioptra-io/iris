from iris.agent.backend.atlas import atlas_backend
from iris.agent.backend.caracal import caracal_backend

__all__ = (
    "atlas_backend",
    "caracal_backend",
)

backend_from_string = {"atlas": atlas_backend, "caracal": caracal_backend}
