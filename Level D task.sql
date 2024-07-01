-- Drop procedure if exists
IF OBJECT_ID('AllocateSubjects', 'P') IS NOT NULL
    DROP PROCEDURE AllocateSubjects;
GO

-- Check and create tables if they do not exist
IF OBJECT_ID('StudentPreference', 'U') IS NULL
BEGIN
    CREATE TABLE StudentPreference (
        Studentid VARCHAR(10) NOT NULL,
        Subjectid VARCHAR(10) NOT NULL,
        Preference INT NOT NULL,
        PRIMARY KEY (Studentid, Subjectid, Preference)
    );
END;
GO

IF OBJECT_ID('SubjectDetails', 'U') IS NULL
BEGIN
    CREATE TABLE SubjectDetails (
        Subjectid VARCHAR(10) NOT NULL,
        SubjectName VARCHAR(255) NOT NULL,
        MaxSeats INT NOT NULL,
        RemainingSeats INT NOT NULL,
        PRIMARY KEY (Subjectid)
    );
END;
GO

IF OBJECT_ID('StudentDetails', 'U') IS NULL
BEGIN
    CREATE TABLE StudentDetails (
        Studentid VARCHAR(10) NOT NULL,
        StudentName VARCHAR(255) NOT NULL,
        GPA DECIMAL(3,2) NOT NULL,
        Branch VARCHAR(255) NOT NULL,
        Section VARCHAR(1) NOT NULL,
        PRIMARY KEY (Studentid)
    );
END;
GO

IF OBJECT_ID('Allotments', 'U') IS NULL
BEGIN
    CREATE TABLE Allotments (
        Subjectid VARCHAR(10) NOT NULL,
        Studentid VARCHAR(10) NOT NULL,
        PRIMARY KEY (Subjectid, Studentid)
    );
END;
GO

IF OBJECT_ID('UnallotedStudents', 'U') IS NULL
BEGIN
    CREATE TABLE UnallotedStudents (
        Studentid VARCHAR(10) NOT NULL,
        PRIMARY KEY (Studentid)
    );
END;
GO

-- Insert sample data
IF NOT EXISTS (SELECT 1 FROM StudentPreference)
BEGIN
    INSERT INTO StudentPreference (Studentid, Subjectid, Preference) VALUES
    ('159103036', 'PO1491', 1),
    ('159103036', 'PO1492', 2),
    ('159103036', 'PO1493', 3),
    ('159103036', 'PO1494', 4),
    ('159103036', 'PO1495', 5),
    ('159103037', 'PO1491', 1),
    ('159103037', 'PO1492', 2),
    ('159103037', 'PO1493', 3),
    ('159103037', 'PO1494', 4),
    ('159103037', 'PO1495', 5),
    ('159103038', 'PO1491', 1),
    ('159103038', 'PO1492', 2),
    ('159103038', 'PO1493', 3),
    ('159103038', 'PO1494', 4),
    ('159103038', 'PO1495', 5),
    ('159103039', 'PO1491', 1),
    ('159103039', 'PO1492', 2),
    ('159103039', 'PO1493', 3),
    ('159103039', 'PO1494', 4),
    ('159103039', 'PO1495', 5),
    ('159103040', 'PO1491', 1),
    ('159103040', 'PO1492', 2),
    ('159103040', 'PO1493', 3),
    ('159103040', 'PO1494', 4),
    ('159103040', 'PO1495', 5),
    ('159103041', 'PO1491', 1),
    ('159103041', 'PO1492', 2),
    ('159103041', 'PO1493', 3),
    ('159103041', 'PO1494', 4),
    ('159103041', 'PO1495', 5);
END;
GO

IF NOT EXISTS (SELECT 1 FROM SubjectDetails)
BEGIN
    INSERT INTO SubjectDetails (Subjectid, SubjectName, MaxSeats, RemainingSeats) VALUES
    ('PO1491', 'Basics of Political Science', 60, 2),
    ('PO1492', 'Basics of Accounting', 120, 119),
    ('PO1493', 'Basics of Financial Markets', 90, 90),
    ('PO1494', 'Eco philosophy', 60, 50),
    ('PO1495', 'Automotive Trends', 60, 60);
END;
GO

IF NOT EXISTS (SELECT 1 FROM StudentDetails)
BEGIN
    INSERT INTO StudentDetails (Studentid, StudentName, GPA, Branch, Section) VALUES
    ('159103036', 'Mohit Agarwal', 8.9, 'CCE', 'A'),
    ('159103037', 'Rohit Agarwal', 5.2, 'CCE', 'A'),
    ('159103038', 'Shohit Garg', 7.1, 'CCE', 'B'),
    ('159103039', 'Mrinal Malhotra', 7.9, 'CCE', 'A'),
    ('159103040', 'Mehreet Singh', 5.6, 'CCE', 'A'),
    ('159103041', 'Arjun Tehlan', 9.2, 'CCE', 'B');
END;
GO

-- Stored procedure to allocate subjects
CREATE PROCEDURE AllocateSubjects
AS
BEGIN
    DECLARE @student_id VARCHAR(10);
    DECLARE @student_gpa DECIMAL(3,2);
    DECLARE @subject_id VARCHAR(10);
    DECLARE @remaining_seats INT;
    DECLARE @allocated INT;
    DECLARE @pref INT;
    
    DECLARE student_cursor CURSOR FOR 
        SELECT Studentid, GPA FROM StudentDetails ORDER BY GPA DESC;

    OPEN student_cursor;
    FETCH NEXT FROM student_cursor INTO @student_id, @student_gpa;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @allocated = 0;
        SET @pref = 1;
        
        WHILE @pref <= 5 AND @allocated = 0
        BEGIN
            -- Initialize variables
            SET @subject_id = NULL;
            SET @remaining_seats = NULL;

            -- Check if the subject is available
            SELECT TOP 1 @subject_id = sp.Subjectid, @remaining_seats = sd.RemainingSeats 
            FROM StudentPreference sp
            JOIN SubjectDetails sd ON sp.Subjectid = sd.Subjectid
            WHERE sp.Studentid = @student_id AND sp.Preference = @pref;

            IF @remaining_seats IS NOT NULL AND @remaining_seats > 0
            BEGIN
                INSERT INTO Allotments (Subjectid, Studentid) VALUES (@subject_id, @student_id);
                UPDATE SubjectDetails SET RemainingSeats = RemainingSeats - 1 WHERE Subjectid = @subject_id;
                SET @allocated = 1;
            END

            SET @pref = @pref + 1;
        END
        
        IF @allocated = 0
        BEGIN
            INSERT INTO UnallotedStudents (Studentid) VALUES (@student_id);
        END

        FETCH NEXT FROM student_cursor INTO @student_id, @student_gpa;
    END

    CLOSE student_cursor;
    DEALLOCATE student_cursor;
END;
GO

SELECT * FROM Allotments;
SELECT * FROM UnallotedStudents;