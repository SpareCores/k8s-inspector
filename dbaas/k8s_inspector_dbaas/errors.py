"""Errors a vendor module raises that the controller must tell from other create failures."""

REGION_UNAVAILABLE = "DBAAS_REGION_UNAVAILABLE:"


class RegionUnavailable(RuntimeError):
    """The vendor cannot create this database in this region, whatever else happens (the offer is restricted for the
    account, the SKU or its quota is missing there). The message starts with `REGION_UNAVAILABLE`, which survives
    Pulumi's wrapping of the exception: the controller then tries another region next time."""

    def __init__(self, message: str) -> None:
        super().__init__(f"{REGION_UNAVAILABLE} {message}")
