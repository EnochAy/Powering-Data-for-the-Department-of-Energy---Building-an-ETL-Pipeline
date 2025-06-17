import pandas as pd
data = {
    "period": ["202301", "202301", "202302", "202302", "202303"],
    "stateid": ["CA", "NY", "CA", "NY", "TX"],
    "stateDescription": ["California", "New York", "California", "New York", "Texas"],
    "sectorid": ["RES", "TRA", "COM", "RES", "TRA"],
    "sectorName": ["residential", "transportation", "commercial", "residential", "transportation"],
    "price": [15.5, 14.2, 16.0, None, 13.8],
    "price-units": ["cents per kWh"] * 5
}
pd.DataFrame(data).to_csv("C:\\Users\\ayomi\\Documents\\Powering Data for the Department of Energy - Building an ETL Pipeline\\electricity_sales.csv", index=False)