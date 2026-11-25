"""Errors a vendor module raises that the controller must tell from other create failures."""

REGION_UNAVAILABLE = "DBAAS_REGION_UNAVAILABLE:"
VENDOR_WIDE = "[vendor-wide]"  # after the marker: no database of the vendor can be created in the region


class RegionUnavailable(RuntimeError):
    """The vendor cannot create this database in this region, whatever else happens (the offer is restricted for the
    account, the SKU or its quota is missing there). The message starts with `REGION_UNAVAILABLE`, which survives
    Pulumi's wrapping of the exception: the controller then tries another region next time."""

    def __init__(self, message: str) -> None:
        super().__init__(f"{REGION_UNAVAILABLE} {message}")


class VendorRegionUnavailable(RegionUnavailable):
    """... and no other database of the vendor can either (the offer is restricted for the whole account in the
    region): the controller also blocks the region for the vendor's other databases for a while."""

    def __init__(self, message: str) -> None:
        super().__init__(f"{VENDOR_WIDE} {message}")
