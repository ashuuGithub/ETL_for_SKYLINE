-- without relationships between the tables


-- Calendar Table
drop table if exists Calendar;
CREATE TABLE Calendar (
    calendarID INT AUTO_INCREMENT NOT NULL,
    districtID INT NOT NULL,
    schoolID INT NOT NULL,
    endYear SMALLINT NOT NULL,
    name VARCHAR(30) NOT NULL,
    number VARCHAR(3) NULL,
    startDate DATETIME NULL,
    endDate DATETIME NULL,
    comments VARCHAR(255) NULL,
    `exclude` BOOLEAN NOT NULL,
    summerSchool BOOLEAN NOT NULL,
    studentDay SMALLINT NULL,
    teacherDay SMALLINT NULL,
    wholeDayAbsence SMALLINT NULL,
    halfDayAbsence SMALLINT NULL,
    calendarGUID CHAR(36) NOT NULL,
    alternativeCode VARCHAR(1) NULL,
    title3 BOOLEAN NOT NULL,
    title3consortium VARCHAR(50) NULL,
    title1 VARCHAR(2) NULL,
    legacyKey INT NULL,
    schoolChoice BOOLEAN NULL,
    `type` VARCHAR(4) NULL,
    countDate DATETIME NULL,
    assignmentRequired BOOLEAN NOT NULL,
    sifExclude BOOLEAN NOT NULL,
    positiveAttendanceEnabled BOOLEAN NOT NULL,
    positiveAttendanceEditDays INT NOT NULL,
    track VARCHAR(3) NULL,
    typeBIE VARCHAR(2) NULL,
    `sequence` SMALLINT NULL,
    externalLMSExclude BOOLEAN NOT NULL,
    attendanceType VARCHAR(2) NULL,
    echs BOOLEAN NOT NULL,
    stem BOOLEAN NOT NULL,
    programType VARCHAR(15) NULL,
    deleteIndicator VARCHAR(1) NULL,
    deleteOrigin VARCHAR(1) NULL,
    deleteReasonCode VARCHAR(5) NULL,
    deleteReasonComments VARCHAR(100) NULL,
    deleteRequestedByID INT NULL,
    deleteRequestedTimestamp DATETIME NULL,
    deleteStatus VARCHAR(1) NULL,
    deleteFailureReason TEXT NULL,
    deleteRequestedByGUID CHAR(36) NULL,
    foodServiceEnrollOverride SMALLINT NOT NULL,
    secondarySchool INT NULL,
    `virtual` BOOLEAN NOT NULL,
    ignoreCourseMasterPush BOOLEAN NOT NULL,
    rolledForwardID INT NULL,
    crossSiteEnrollmentOpen BOOLEAN NOT NULL,
    PRIMARY KEY (calendarID),
    UNIQUE (calendarGUID),
    INDEX IX_Calendar_districtID (districtID ASC, schoolID ASC, calendarID ASC),
    INDEX IX_Calendar_endYear (endYear ASC, districtID ASC, schoolID ASC, name ASC),
    INDEX IX_Calendar_schoolID (schoolID ASC, endYear ASC, calendarID ASC)
);







drop table if exists Enrollment;
CREATE TABLE Enrollment (
    enrollmentID INT AUTO_INCREMENT NOT NULL,
    personID INT NOT NULL,
    calendarID INT NOT NULL,
    structureID INT NOT NULL,
    grade VARCHAR(4) NULL,
    serviceType VARCHAR(1) NOT NULL,
    active BOOLEAN NOT NULL,
    classRankExclude BOOLEAN NOT NULL,
    noShow BOOLEAN NOT NULL,
    startDate DATETIME NOT NULL,
    startStatus VARCHAR(5) NULL,
    startComments VARCHAR(250) NULL,
    endDate DATETIME NULL,
    endStatus VARCHAR(5) NULL,
    endComments VARCHAR(250) NULL,
    endAction VARCHAR(15) NULL,
    nextCalendar INT NOT NULL,
    nextGrade VARCHAR(4) NULL,
    diplomaDate DATETIME NULL,
    diplomaType VARCHAR(3) NULL,
    diplomaPeriod VARCHAR(3) NULL,
    postGradPlans VARCHAR(3) NULL,
    postGradLocation VARCHAR(2) NULL,
    gradYear SMALLINT NULL,
    stateExclude BOOLEAN NOT NULL,
    servingDistrict VARCHAR(12) NULL,
    residentDistrict VARCHAR(12) NULL,
    residentSchool VARCHAR(10) NULL,
    mealStatus VARCHAR(4) NULL,
    englishProficiency VARCHAR(4) NULL,
    englishProficiencyDate DATETIME NULL,
    lep VARCHAR(50) NULL,
    esl VARCHAR(5) NULL,
    `language` VARCHAR(4) NULL,
    citizenship VARCHAR(4) NULL,
    title1 VARCHAR(2) NULL,
    title3 VARCHAR(2) NULL,
    transportation VARCHAR(2) NULL,
    migrant VARCHAR(2) NULL,
    immigrant VARCHAR(3) NULL,
    homeless VARCHAR(2) NULL,
    homeSchooled VARCHAR(1) NULL,
    homebound VARCHAR(1) NULL,
    giftedTalented VARCHAR(2) NULL,
    nclbChoice VARCHAR(2) NULL,
    percentEnrolled DECIMAL(7,3) NULL,
    admOverride DECIMAL(7,3) NULL,
    adaOverride DECIMAL(7,3) NULL,
    vocationalCode VARCHAR(2) NULL,
    pseo VARCHAR(4) NULL,
    facilityCode VARCHAR(6) NULL,
    stateAid VARCHAR(10) NULL,
    stateFundingCode VARCHAR(2) NULL,
    section504 VARCHAR(2) NULL,
    specialEdStatus VARCHAR(5) NULL,
    specialEdSetting VARCHAR(6) NULL,
    disability1 VARCHAR(15) NULL,
    disability2 VARCHAR(15) NULL,
    disability3 VARCHAR(15) NULL,
    disability4 VARCHAR(15) NULL,
    disability5 VARCHAR(15) NULL,
    enrollmentGUID CHAR(36) NOT NULL,
    privateSchooled VARCHAR(1) NULL,
    spedExitDate DATETIME NULL,
    spedExitReason VARCHAR(3) NULL,
    childCountStatus VARCHAR(2) NULL,
    grade9Date DATETIME NULL,
    singleParent VARCHAR(1) NULL,
    displacedHomemaker VARCHAR(1) NULL,
    legacyKey VARCHAR(35) NULL,
    legacySeq1 SMALLINT NULL,
    legacySeq2 SMALLINT NULL,
    endYear SMALLINT NOT NULL,
    districtID INT NOT NULL,
    localStudentNumber VARCHAR(15) NULL,
    modifiedDate DATETIME NULL,
    modifiedByID INT NULL,
    dropoutCode VARCHAR(15) NULL,
    eip VARCHAR(1) NULL,
    adult BOOLEAN NOT NULL,
    servingCounty VARCHAR(8) NULL,
    attendanceGroup VARCHAR(3) NULL,
    projectedGraduationDate DATETIME NULL,
    withdrawDate DATETIME NULL,
    rollForwardCode VARCHAR(1) NULL,
    rollForwardEnrollmentID INT NULL,
    cohortYear VARCHAR(4) NULL,
    disability6 VARCHAR(15) NULL,
    disability7 VARCHAR(15) NULL,
    disability8 VARCHAR(15) NULL,
    disability9 VARCHAR(15) NULL,
    disability10 VARCHAR(15) NULL,
    nextStructureID INT NULL,
    schoolEntryDate DATETIME NULL,
    districtEntryDate DATETIME NULL,
    mvUnaccompaniedYouth VARCHAR(2) NULL,
    externalLMSExclude BOOLEAN NOT NULL,
    schoolOfAccountability INT NULL,
    localStartStatusTypeID INT NULL,
    localEndStatusTypeID INT NULL,
    schoolChoiceProgram VARCHAR(2) NULL,
    dpsaCalculatedTier VARCHAR(15) NULL,
    dpsaReportedTier VARCHAR(15) NULL,
    excludeFromDpsaCalculation BOOLEAN NOT NULL,
    crossSiteEnrollment BOOLEAN NOT NULL,
    peerID INT NULL,
    choiceBasisReason VARCHAR(15) NULL,
    PRIMARY KEY (enrollmentID),
    UNIQUE (enrollmentGUID),
    INDEX IX_Enrollment_endYear_districtID (endYear ASC, districtID ASC),
    INDEX IX_Enrollment_modifiedDate_noShow (modifiedDate ASC, noShow ASC),
    INDEX IX_Enrollment_personID (personID ASC)
);








drop table if exists Identity;
CREATE TABLE Identity (
    identityID INT AUTO_INCREMENT NOT NULL,
    personID INT NOT NULL,
    effectiveDate DATETIME NULL,
    lastName VARCHAR(50) NOT NULL,
    firstName VARCHAR(50) NULL,
    middleName VARCHAR(50) NULL,
    suffix VARCHAR(50) NULL,
    alias VARCHAR(50) NULL,
    gender CHAR(1) NULL,
    birthdate DATETIME NULL,
    ssn VARCHAR(9) NULL,
    raceEthnicity VARCHAR(3) NULL,
    birthCountry VARCHAR(4) NULL,
    dateEnteredUS DATETIME NULL,
    birthVerification VARCHAR(4) NULL,
    comments VARCHAR(500) NULL,
    districtID INT NOT NULL,
    hispanicEthnicity VARCHAR(2) NULL,
    identityGUID CHAR(36) NULL,
    lastNamePhonetic VARCHAR(10) NULL,
    firstNamePhonetic VARCHAR(10) NULL,
    dateEnteredState DATETIME NULL,
    birthCertificate VARCHAR(12) NULL,
    immigrant VARCHAR(2) NULL,
    dateEnteredUSSchool DATETIME NULL,
    raceEthnicityFed VARCHAR(1) NULL,
    raceEthnicityDetermination VARCHAR(2) NULL,
    birthStateNoSIF VARCHAR(2) NULL,
    birthCity VARCHAR(30) NULL,
    birthCounty VARCHAR(30) NULL,
    modifiedByID INT NULL,
    modifiedDate DATETIME NULL,
    birthVerificationBIE VARCHAR(4) NULL,
    refugee VARCHAR(2) NULL,
    homePrimaryLanguage VARCHAR(5) NULL,
    stateHispanicEthnicity VARCHAR(2) NULL,
    birthState VARCHAR(10) NULL,
    homePrimaryLanguageBIE VARCHAR(5) NULL,
    homeSecondaryLanguageBIE VARCHAR(5) NULL,
    languageAlt VARCHAR(5) NULL,
    languageAlt2 VARCHAR(5) NULL,
    foreignLanguageProficiency BOOLEAN NOT NULL,
    literacyLanguage BOOLEAN NOT NULL,
    legalFirstName NVARCHAR(50) NULL,
    legalLastName NVARCHAR(50) NULL,
    legalMiddleName NVARCHAR(50) NULL,
    legalSuffix VARCHAR(50) NULL,
    legalGender CHAR(1) NULL,
    usCitizen VARCHAR(15) NULL,
    visaType VARCHAR(5) NULL,
    originCountry VARCHAR(4) NULL,
    hispanicWriteIn VARCHAR(50) NULL,
    asianWriteIn VARCHAR(50) NULL,
    caribbeanWriteIn VARCHAR(50) NULL,
    centralAfricanWriteIn VARCHAR(50) NULL,
    eastAfricanWriteIn VARCHAR(50) NULL,
    latinAmericanWriteIn VARCHAR(50) NULL,
    southAfricanWriteIn VARCHAR(50) NULL,
    westAfricanWriteIn VARCHAR(50) NULL,
    blackWriteIn VARCHAR(50) NULL,
    alaskaNativeWriteIn VARCHAR(50) NULL,
    americanIndianWriteIn VARCHAR(50) NULL,
    pacificIslanderWriteIn VARCHAR(50) NULL,
    easternEuropeanWriteIn VARCHAR(50) NULL,
    middleEasternWriteIn VARCHAR(50) NULL,
    northAfricanWriteIn VARCHAR(50) NULL,
    usEntryType VARCHAR(15) NULL,
    multipleBirth BOOLEAN NOT NULL,
    languageSurveyDate DATE NULL,
    certificateOfIndianBlood BOOLEAN NOT NULL,
    birthGender CHAR(1) NULL,
    languageInterpreter BOOLEAN NOT NULL,
    languageAltInterpreter BOOLEAN NOT NULL,
    languageAlt2Interpreter BOOLEAN NOT NULL,
    educationLevel VARCHAR(2) NULL,
    pronounID INT NULL,
    tribalEnrollment VARCHAR(15) NULL,
    languageAlt3 VARCHAR(5) NULL,
    languageAlt4 VARCHAR(5) NULL,
    PRIMARY KEY (identityID),
    INDEX IX_Identity_DistrictID (districtID ASC),
    UNIQUE IX_Identity_GUID (identityGUID ASC),
    INDEX IX_Identity_birthdate (birthdate ASC),
    INDEX IX_Identity_lastName (lastName ASC, firstName ASC),
    INDEX IX_Identity_modifiedDate (modifiedDate ASC),
    INDEX IX_Identity_personID (personID ASC),
    INDEX IX_Identity_ssn (ssn ASC),
    INDEX Identity_Phonetic (lastNamePhonetic ASC, firstNamePhonetic ASC)
);





drop table if exists Person;
CREATE TABLE Person (
    personID INT AUTO_INCREMENT NOT NULL,
    currentIdentityID INT NULL,
    stateID VARCHAR(15) NULL,
    studentNumber VARCHAR(15) NULL,
    staffNumber VARCHAR(15) NULL,
    personGUID CHAR(36) NOT NULL,
    legacyKey VARCHAR(201) NULL,
    otherID VARCHAR(15) NULL,
    staffStateID VARCHAR(20) NULL,
    geographicStaffStateID VARCHAR(20) NULL,
    modifiedByID INT NOT NULL,
    comments VARCHAR(20) NULL,
    additionalID VARCHAR(25) NULL,
    edFiID VARCHAR(20) NULL,
    PRIMARY KEY (personID),
    UNIQUE (personGUID),
    INDEX IX_Person_additionalID (additionalID ASC),
    INDEX IX_Person_currentIdentityID (currentIdentityID ASC),
    INDEX IX_Person_edFiID (edFiID ASC),
    INDEX IX_Person_staffNumber (staffNumber ASC),
    INDEX IX_Person_staffStateID (staffStateID ASC),
    INDEX IX_Person_stateID (stateID ASC),
    INDEX IX_Person_studentNumber (studentNumber ASC)
);




drop table if exists Pronoun;
CREATE TABLE Pronoun (
    pronounID INT AUTO_INCREMENT NOT NULL,
    code VARCHAR(15) NOT NULL,
    stateCode VARCHAR(15) NULL,
    name VARCHAR(256) NOT NULL,
    startDate DATE NULL,
    endDate DATE NULL,
    subjectiveForm VARCHAR(30) NULL,
    objectiveForm VARCHAR(30) NULL,
    dependentPossessiveForm VARCHAR(30) NULL,
    independentPossessiveForm VARCHAR(30) NULL,
    PRIMARY KEY (pronounID)
);



ALTER TABLE Calendar
    ADD CONSTRAINT FK_Calendar_School FOREIGN KEY (schoolID) REFERENCES School(schoolID);

ALTER TABLE Enrollment
    ADD CONSTRAINT FK_Enrollment_Calendar FOREIGN KEY (calendarID) REFERENCES Calendar(calendarID),
    ADD CONSTRAINT FK_Enrollment_Person FOREIGN KEY (personID) REFERENCES Person(personID);

ALTER TABLE `Identity`
    ADD CONSTRAINT FK_Identity_Person FOREIGN KEY (personID) REFERENCES Person(personID);
--     ADD CONSTRAINT FK_Identity_Pronoun FOREIGN KEY (pronounID) REFERENCES Pronoun(pronounID);

ALTER TABLE Person
    ADD CONSTRAINT FK_Person_Identity FOREIGN KEY (currentIdentityID) REFERENCES `Identity`(identityID);








