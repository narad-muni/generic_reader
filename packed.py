def calculate_packed_size(fields, alignment=2):
    current_offset = 0
    total_size = 0
    layout = []

    for field_name, field_size in fields.items():
        # Calculate padding
        padding = (alignment - (current_offset % alignment)) % alignment
        layout.append((field_name, current_offset, field_size, padding))
        
        # Update current offset
        current_offset += field_size + padding
        total_size = current_offset

    return layout, total_size

# Example usage
fields = {
    "filler": 4,
    "logtime": 4,
    "alpha_char": 2,
    "transaction_code": 2,
    "error_code": 2,
    "filler": 26,
    "token": 4,
    "InstrumentName": 6,
    "Symbol": 10,
    "Series": 2,
    "expiry date": 4,
    "strike price": 4,
    "option type": 2,
    "ca level": 2,
    "PermittedToTrade": 2,
    "IssuedCapital": 8,
    "WarningQuantity": 4,
    "FreezeQuantity": 4,
    "CreditRating": 12,
    "eligibilty": 1,
    "status": 2,
    "eligibilty": 1,
    "status": 2,
    "eligibilty": 1,
    "status": 2,
    "IssueRate": 2,
    "IssueStartDate": 4,
    "InterestPaymentDate": 4,
    "IssueMaturityDate": 4,
    "MarginPercentage": 4,
    "MinimumLotQuantity": 4,
    "BoardLotQuantity": 4,
    "TickSize": 4,
    "Name": 25,
    "Reserved": 1,
    "ListingDate": 4,
    "ExpulsionDate": 4,
    "ReAdmissionDate": 4,
    "RecordDate": 4,
    "LowPriceRange": 4,
    "HighPriceRange": 4,
    "ExpiryDate": 4,
    "NoDeliveryStartDate": 4,
    "NoDeliveryEndDate": 4,
    "BookClosureStartDate": 4,
    "BookClosureEndDate": 4,
    "ExerciseStartDate": 4,
    "ExerciseEndDate": 4,
    "OldToken": 4,
    "AssetInstrument": 6,
    "AssetName": 10,
    "AssetToken": 4,
    "IntrinsicValue": 4
}
layout, total_size = calculate_packed_size(fields)
for field in layout:
    print(f"Field {field[0]} at offset {field[1]} with size {field[2]}, padding {field[3]}")
print(f"Total packed size: {total_size} bytes")