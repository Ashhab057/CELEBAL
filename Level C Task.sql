SELECT Start_Date, min(End_Date)
FROM 
 (SELECT Start_Date FROM Projects WHERE Start_Date NOT IN (SELECT End_Date FROM Projects)) a ,
 (SELECT End_Date FROM Projects WHERE End_Date NOT IN (SELECT Start_Date FROM Projects)) b
WHERE Start_Date < End_Date
GROUP BY Start_Date
ORDER BY DATEDIFF(min(End_Date), Start_Date) ASC, Start_Date ASC;


Select S.Name
From ( Students S join Friends F using(ID)
 join Packages P1 on S.ID=P1.ID
 join Packages P2 on F.Friend_ID=P2.ID)
Where P2.Salary > P1.Salary
Order By P2.Salary;


SELECT X1, Y1 FROM 
(SELECT F1.X AS X1, F1.Y AS Y1, F2.X AS X2, F2.Y AS Y2 FROM Functions F1 
INNER JOIN Functions F2 ON F1.X=F2.Y AND F1.Y=F2.X 
ORDER BY F1.X) AS A
GROUP BY X1, Y1 
HAVING COUNT(X1)>1 OR X1<Y1 
ORDER BY X1;


SELECT c.contest_id, c.hacker_id, c.name, SUM(COALESCE(s.total_submissions, 0)) AS 
total_submissions, SUM(COALESCE(s.total_accepted_submissions, 0)) AS total_accepted_submissions, 
SUM(COALESCE(v.total_views, 0)) AS total_views, SUM(COALESCE(v.total_unique_views, 0)) 
AS total_unique_views FROM contests c JOIN colleges col ON c.contest_id = col.contest_id JOIN 
challenges chal ON col.college_id = chal.college_id LEFT JOIN ( SELECT challenge_id, 
SUM(total_submissions) AS total_submissions, SUM(total_accepted_submissions) AS
total_accepted_submissions FROM Submission_Stats GROUP BY challenge_id ) s ON 
chal.challenge_id = s.challenge_id LEFT JOIN ( SELECT challenge_id, SUM(total_views)
AS total_views, SUM(total_unique_views) AS total_unique_views FROM view_stats GROUP BY
challenge_id ) v ON chal.challenge_id = v.challenge_id GROUP BY c.contest_id, c.hacker_id, 
c.name HAVING (SUM(COALESCE(s.total_submissions, 0)) + SUM(COALESCE(s.total_accepted_submissions, 0)) 
+ SUM(COALESCE(v.total_views, 0)) + SUM(COALESCE(v.total_unique_views, 0))) > 0 ORDER BY c.contest_id;




with derived as ( SELECT submission_date, hacker_id, DENSE_RANK () 
OVER(PARTITION BY submission_date order by submission_date,count(hacker_id)
desc , hacker_id) as rnk FROM Submissions group by submission_date, hacker_id),
derived2 as( Select submission_date,a.hacker_id,a.name from Hackers a 
inner join derived b on a.hacker_id = b.hacker_id where b.rnk = 1 order by b.submission_date),
derived3 as(SELECT hacker_id, submission_date, DENSE_RANK() OVER(PARTITION BY hacker_id order by submission_date)
as rpt FROM Submissions order by submission_date), derived4 as( select submission_date,count(distinct hacker_id)
as cnt from derived3 where rpt = EXTRACT(DAY FROM submission_date) 
group by submission_date) Select a.submission_date,b.cnt,a.hacker_id,a.name from derived2 a join derived4 b on a.submission_date = b.submission_date;

SELECT CITY, STATE FROM STATION;


WITH recursive cte_numgen AS (
    SELECT 2 AS n
    UNION ALL
    SELECT n+1 FROM cte_numgen WHERE n < 1000
)
SELECT 
    GROUP_CONCAT(a.n SEPARATOR '&') AS prime_string
FROM cte_numgen a
WHERE NOT  EXISTS (
    SELECT 1
    FROM cte_numgen b
    WHERE a.n > b.n AND a.n % b.n = 0
)

SELECT MAX(CASE WHEN occupation='doctor' THEN NAME ELSE NULL END) AS
doctor, MAX(CASE WHEN occupation='professor' THEN NAME ELSE NULL END)
AS professor, MAX(CASE WHEN occupation='singer' THEN NAME ELSE NULL END)AS
singer, MAX(CASE WHEN occupation='actor' THEN NAME ELSE NULL END)AS actor 
FROM ( select name , occupation, ROW_NUMBER() over
(partition by occupation order by name) as rowno from occupations )as subtb group by rowno order by rowno



select N,
    case
        when P is null then 'Root'
        when N  in (select P from BST) then 'Inner'
        else 'Leaf'
    end
from BST order by N;


SELECT 
     C.company_code,
     C.founder,
     COUNT(DISTINCT E.lead_manager_code),
     COUNT(DISTINCT E.senior_manager_code),
     COUNT(DISTINCT E.manager_code),
     COUNT(DISTINCT E.employee_code)
FROM Company C
   INNER JOIN Employee E
        ON C.company_code = E.company_code
GROUP BY C.company_code, C.founder
ORDER BY C.company_code;

SELECT 
    pro_com, 
    SUM(pro_price) AS total_cost, 
    SUM(CASE WHEN pro_country = 'India' THEN pro_price ELSE 0 END) AS india_cost, 
    SUM(CASE WHEN pro_country = 'International' THEN pro_price ELSE 0 END) AS international_cost, 
    ROUND(SUM(CASE WHEN pro_country = 'India' THEN pro_price ELSE 0 END) / SUM(pro_price) * 100, 2) AS india_ratio, 
    ROUND(SUM(CASE WHEN pro_country = 'International' THEN pro_price ELSE 0 END) / SUM(pro_price) * 100, 2) AS international_ratio
FROM 
    item_mast
GROUP BY 
    pro_com;





WITH monthly_cost AS (
  SELECT 
    DATE_TRUNC('month', Order_date) AS month,
    SUM(Cost) AS cost
  FROM 
    Products
  GROUP BY 
    DATE_TRUNC('month', Order_date)
),
monthly_revenue AS (
  SELECT 
    DATE_TRUNC('month', Order_date) AS month,
    SUM(Sales) AS revenue
  FROM 
    Products
  GROUP BY 
    DATE_TRUNC('month', Order_date)
)
SELECT 
  mc.month,
  mc.cost,
  mr.revenue,
  (mc.cost / mr.revenue) * 100 AS cost_revenue_ratio,
  (mc.cost - LAG(mc.cost) OVER (ORDER BY mc.month ASC)) / LAG(mc.cost) OVER (ORDER BY mc.month ASC) * 100 AS cost_growth,
  (mr.revenue - LAG(mr.revenue) OVER (ORDER BY mr.month ASC)) / LAG(mr.revenue) OVER (ORDER BY mr.month ASC) * 100 AS revenue_growth
FROM 
  monthly_cost mc
  JOIN monthly_revenue mr ON mc.month = mr.month
ORDER BY 
  mc.month;





  WITH ranked_employees AS (
  SELECT 
    emp_name, 
    emp_salary, 
    DENSE_RANK() OVER (ORDER BY emp_salary DESC) AS rank
  FROM 
    emp
)
SELECT 
  emp_name, 
  emp_salary
FROM 
  ranked_employees
WHERE 
  rank <= 5;


  CREATE LOGIN [new_user] WITH PASSWORD = 'strong_password';
  USE [database_name];
CREATE USER [new_user] FOR LOGIN [new_user];

-- Create a login
CREATE LOGIN [new_user] WITH PASSWORD = 'strong_password';

-- Create a user
USE [database_name];
CREATE USER [new_user] FOR LOGIN [new_user];

-- Add the user to the db_owner role
ALTER ROLE [db_owner] ADD MEMBER [new_user];




WITH MonthlyEmployeeCost AS (
  SELECT 
    BU,
    MONTH(StartDate) AS Month,
    SUM(Cost) AS TotalCost,
    SUM(Headcount) AS TotalHeadcount
  FROM 
    Projects
  GROUP BY 
    BU, MONTH(StartDate)
)
SELECT 
  BU,
  Month,
  SUM(TotalCost * TotalHeadcount) / SUM(TotalHeadcount) AS WeightedAverageCost
FROM 
  MonthlyEmployeeCost
GROUP BY 
  BU, Month
ORDER BY 
  BU, Month;




	SELECT 
    Sub_Band, 
    COUNT(*) AS Headcount, 
    ROUND(COUNT(*) / (SELECT COUNT(*) FROM Projects) * 100, 2) AS Percentage
FROM 
    Projects
GROUP BY 
    Sub_Band
ORDER BY 
    Sub_Band;

19:-
SELECT CAST(CEILING(AVG(CAST(Salary AS FLOAT)) - AVG(CAST(REPLACE(Salary,0,'') AS FLOAT))) AS INT)
FROM EMPLOYEES;

20:-

INSERT INTO new_table (column1, column2, column3) 
SELECT column1, column2, column3 
FROM old_table;
