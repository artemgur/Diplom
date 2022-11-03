# Generating sample data
import pandas as pd

ColumnNames = ['FuelType', 'CarPrice']
DataValues = [['Petrol', 2000],
              ['Petrol', 2100],
              ['Petrol', 1900],
              ['Petrol', 2150],
              ['Petrol', 2100],
              ['Petrol', 2200],
              ['Petrol', 1950],
              ['Diesel', 2500],
              ['Diesel', 2700],
              ['Diesel', 2900],
              ['Diesel', 2850],
              ['Diesel', 2600],
              ['Diesel', 2500],
              ['Diesel', 2700],
              ['CNG', 1500],
              ['CNG', 1400],
              ['CNG', 1600],
              ['CNG', 1650],
              ['CNG', 1600],
              ['CNG', 1500],
              ['CNG', 1500]

              ]
# Create the Data Frame
CarData = pd.DataFrame(data=DataValues, columns=ColumnNames)

if __name__ == '__main__':
    print(CarData.head())
    ########################################################
    # f_oneway() function takes the group data as input and
    # returns F-statistic and P-value
    from scipy.stats import f_oneway

    # Running the one-way anova test between CarPrice and FuelTypes
    # Assumption(H0) is that FuelType and CarPrices are NOT correlated

    # Finds out the Prices data for each FuelType as a list
    CategoryGroupLists = CarData.groupby('FuelType')['CarPrice'].apply(list)
    #print(CategoryGroupLists)
    #print(*CategoryGroupLists)
    # Performing the ANOVA test
    # We accept the Assumption(H0) only when P-Value &gt; 0.05
    AnovaResults = f_oneway(*CategoryGroupLists)
    print('P-Value for Anova is: ', AnovaResults[1])