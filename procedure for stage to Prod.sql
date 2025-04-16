USE Karmada_staging;

DELIMITER //

CREATE PROCEDURE Karmada_staging.ProcessStagingToProduction()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'An error occurred during processing.';
        ROLLBACK;
    END;

    START TRANSACTION;

    -- Process estimates_v1_monday
    INSERT INTO Karmada_Prod.estimates_v1_monday (id, jobId, status_name, soldOn, subtotal, Brand, CompanyLocation)
    SELECT id, jobId, status_name, soldOn, subtotal, Brand, CompanyLocation
    FROM (
        SELECT id, jobId, status_name, soldOn, subtotal, Brand, CompanyLocation,
               ROW_NUMBER() OVER (PARTITION BY CompanyLocation, jobId ORDER BY jobId) AS rn
        FROM Karmada_staging.estimates_v1_monday
    ) t
    WHERE rn = 1;

    -- Process export_jobs_monday
    INSERT INTO Karmada_Prod.export_jobs_monday (id, jobStatus, completedOn, Brand, estimate_subtotal, businessUnitName, CompanyLocation)
    SELECT id, jobStatus, completedOn, Brand, estimate_subtotal, businessUnitName, CompanyLocation
    FROM (
        SELECT id, jobStatus, completedOn, Brand, estimate_subtotal, businessUnitName, CompanyLocation,
               ROW_NUMBER() OVER (PARTITION BY CompanyLocation, id ORDER BY id) AS rn
        FROM Karmada_staging.export_jobs_monday
    ) t
    WHERE rn = 1;

    -- Process invoice
    INSERT INTO Karmada_Prod.invoice (id, referenceNumber, subTotal, total, businessUnitName, active, CompanyLocation, Brand)
    SELECT id, referenceNumber, subTotal, total, businessUnitName, active, CompanyLocation, Brand
    FROM (
        SELECT id, referenceNumber, subTotal, total, businessUnitName, active, CompanyLocation, Brand,
               ROW_NUMBER() OVER (PARTITION BY CompanyLocation, referenceNumber ORDER BY referenceNumber) AS rn
        FROM Karmada_staging.invoice
    ) t
    WHERE rn = 1;

    -- Process payments_v1
    INSERT INTO Karmada_Prod.payments_v1 (active, id, referenceNumber, date, total, Brand, appliedAmount, CompanyLocation)
    SELECT active, id, referenceNumber, date, total, Brand, appliedAmount, CompanyLocation
    FROM (
        SELECT active, id, referenceNumber, date, total, Brand, appliedAmount, CompanyLocation,
               ROW_NUMBER() OVER (PARTITION BY referenceNumber, CompanyLocation ORDER BY referenceNumber) AS rn
        FROM Karmada_staging.payments_v1
    ) t
    WHERE rn = 1;

    COMMIT;

    SELECT 'Data processed successfully for all tables.' AS Message;
END //

DELIMITER ;