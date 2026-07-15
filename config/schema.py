REQUIRED_COLUMNS = {
    "ASN": ["BN", "Expiry Date", "Trade Name", "Received Quantity"],
    "INVENTORY": ["BN", "Expiry Date", "Trade Name", "Available Quantity"],
    "DISPATCH": [
        "BN", "Expiry Date", "Trade Name", "Dispatched Quantity",
        "To Address", "Sales Order Number"
    ],
    "SFDA": [
        "GTIN", "Drug Name", "BN", "Expiry Date", "Quantity", "Active",
        "Quantity Receive Pending", "Quantity sent pending"
    ],
    "PACKSIZE": ["Trade Name", "PackageSize"]
}
