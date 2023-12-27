
import mysql.connector
db = mysql.connector.connect(
            host = "localhost",
            user = "root",
            passwd = "123456789",
            )

cursor = db.cursor()
###############    Q - A    ###############

#query1- the details of the employees (name and age) associated with Tel Aviv
cursor.execute('''SELECT Emp_det.name, Emp_det.age
    FROM Emp_det
    JOIN CityWorkers ON Emp_det.id = CityWorkers.id
    WHERE CityWorkers.city_name = 'Tel Aviv';
''')
results = cursor.fetchall()
# Print the results
for row in results:
    print(f"Name: {row[0]}, Age: {row[1]}")

#query2-the cities and the number of employees associated with them. Show only cities where the average age of employees
# is greater than 25. Sort the table by the names of the cities in ascending order.
cursor.execute('''
    SELECT CityWorkers.city_name, COUNT(Emp_det.employee_id) AS num_employees
    FROM CityWorkers
    JOIN Emp_det ON CityWorkers.worker_id = Emp_det.employee_id
    GROUP BY CityWorkers.city_name
    HAVING AVG(Emp_det.age) > 25
    ORDER BY CityWorkers.city_name ASC;
    ''')
results = cursor.fetchall()
# Print the results
for row in results:
    print(f"City: {row[0]}, Number of Employees: {row[1]}")


###############    Q - B    ###############

#query3-the total sales in the sports and music departments in the first 3 months of 2016, and their relative percentage
# out of the department's total sales that year and the relative percentage out of the company's total sales from the day
# it was founded until today.
cursor.execute('''WITH DepartmentSales AS (
    SELECT
        Department, SUM(Total_Price) AS DepartmentTotalSales
    FROM
        Sales
    WHERE
        EXTRACT(YEAR FROM DATE) = 2016 AND EXTRACT(MONTH FROM DATE) BETWEEN 1 AND 3
    GROUP BY
        Department
    ),
    CompanyTotalSales AS (
        SELECT
            SUM(Total_Price) AS TotalSales
        FROM
            Sales
    )
    SELECT
        Sales.Department,
        SUM(CASE WHEN Sales.Department = 'Sports' THEN Sales.Total_Price ELSE 0 END) AS SportsSales,
        SUM(CASE WHEN Sales.Department = 'Music' THEN Sales.Total_Price ELSE 0 END) AS MusicSales,
        (SUM(CASE WHEN Sales.Department = 'Sports' THEN Sales.Total_Price ELSE 0 END) / DepartmentSales.DepartmentTotalSales) * 100 AS SportsPercentageOfDepartment,
        (SUM(CASE WHEN Sales.Department = 'Music' THEN Sales.Total_Price ELSE 0 END) / DepartmentSales.DepartmentTotalSales) * 100 AS MusicPercentageOfDepartment,
        (SUM(CASE WHEN Sale.Department = 'Sports' THEN Sales.Total_Price ELSE 0 END) / CompanyTotalSales.TotalSales) * 100 AS SportsPercentageOfCompany,
        (SUM(CASE WHEN Sales.Department = 'Music' THEN Sales.Total_Price ELSE 0 END) / CompanyTotalSales.TotalSales) * 100 AS MusicPercentageOfCompany
    FROM
        DepartmentSales
    JOIN
        Sales Sales ON DepartmentSales.Department = Sales.Department
    CROSS JOIN
        CompanyTotalSales
    GROUP BY
        Sales.Department, DepartmentSales.DepartmentTotalSales, CompanyTotalSales.TotalSales;

''')
results = cursor.fetchall()
for row in results:
    print(f"Employee ID: {row[0]}, Name: {row[1]} {row[2]}, Total Sales in 2015: {row[3]}")


#query4-employees with the highest total sales in 2015 who still work for the company.
cursor.execute('''
    WITH EmployeeSales AS (
        SELECT
            COALESCE(Employees.Employee_Id, 999) AS EmployeeId,
            Employees.First_Name,
            Employees.Last_Name,
            SUM(Sales.Total_Price) AS TotalSales2015
        LEFT JOIN
            Sales Sales ON Employees.Employee_Id = Sales.Employee_Id
                      AND EXTRACT(YEAR FROM Sales.DATE) = 2015
        WHERE
            Employees.Employee_Id IS NOT NULL
        GROUP BY
            Employees.Employee_Id, Employees.First_Name, Employees.Last_Name
    )
    SELECT
        EmployeeId, First_Name, Last_Name, TotalSales2015
    FROM
        EmployeeSales
    ORDER BY
        TotalSales2015 DESC
    LIMIT 4;
    ''')
results = cursor.fetchall()
for row in results:
    print(f"Employee ID: {row[0]}, Name: {row[1]} {row[2]}, Total Sales in 2015: {row[3]}")

#q5-Check - valid first and last name

import re
# Check if the name contains only letters and does not start with a digit
def IsValidName(name):
    return bool(re.match("^[A-Za-z]+[A-Za-z']*$", name))
