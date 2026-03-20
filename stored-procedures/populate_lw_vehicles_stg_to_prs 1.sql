-- Ticket: (NWEREP-2830)
CREATE OR REPLACE PROCEDURE kpnwe_ev.populate_lw_vehicles_stg_to_prs()
 LANGUAGE plpgsql
AS $$
BEGIN

    RAISE INFO 'Upsert started: kpnwe_ev.dim_lw_vehicles';

    -- 1) Close old row when tacking column changed (last_modified_date)
    -- note: Adding "IS NOT DISTINCT" instead of '<>'. This is because NULL values can't be matched with NULL, results in new rows being created instead of updating existing row.
    UPDATE 
        kpnwe_ev.dim_lw_vehicles
    SET 
        valid_to = kpnwe_ev_stg.stg_lw_vehicles.last_modified_date,                                                         -- to show difference between old (FALSE) transaction and new (TRUE) transaction 
        is_current = false
    FROM 
        kpnwe_ev_stg.stg_lw_vehicles
    WHERE 
        kpnwe_ev_stg.stg_lw_vehicles.vehicle_key = kpnwe_ev.dim_lw_vehicles.vehicle_key
        AND 
        kpnwe_ev.dim_lw_vehicles.is_current = true
        AND NOT (
                kpnwe_ev.dim_lw_vehicles.last_modified_date IS NOT DISTINCT FROM kpnwe_ev_stg.stg_lw_vehicles.last_modified_date         -- tacking columns  
            );

    -- 2) Type-1 updates for untracked attributes (tracked must match)
    UPDATE 
        kpnwe_ev.dim_lw_vehicles
    SET
        company_id = kpnwe_ev_stg.stg_lw_vehicles.company_id,
        company_brand = kpnwe_ev_stg.stg_lw_vehicles.company_brand,
        model = kpnwe_ev_stg.stg_lw_vehicles.model,
        registration_number = kpnwe_ev_stg.stg_lw_vehicles.registration_number,
        tank_capacity = kpnwe_ev_stg.stg_lw_vehicles.tank_capacity,
        battery_capacity = kpnwe_ev_stg.stg_lw_vehicles.battery_capacity,
        initial_odometer = kpnwe_ev_stg.stg_lw_vehicles.initial_odometer,
        last_odometer = kpnwe_ev_stg.stg_lw_vehicles.last_odometer,
        leasing_start_date = kpnwe_ev_stg.stg_lw_vehicles.leasing_start_date,
        vehicle_date = kpnwe_ev_stg.stg_lw_vehicles.vehicle_date,
        vehicle_type = kpnwe_ev_stg.stg_lw_vehicles.vehicle_type,
        eu_registered = kpnwe_ev_stg.stg_lw_vehicles.eu_registered,
        code = kpnwe_ev_stg.stg_lw_vehicles.code,
        creation_date = kpnwe_ev_stg.stg_lw_vehicles.creation_date,
        co2 = kpnwe_ev_stg.stg_lw_vehicles.co2,
        power_supply = kpnwe_ev_stg.stg_lw_vehicles.power_supply,
        chassis_number = kpnwe_ev_stg.stg_lw_vehicles.chassis_number,
        horse_power = kpnwe_ev_stg.stg_lw_vehicles.horse_power,
        target_consumption = kpnwe_ev_stg.stg_lw_vehicles.target_consumption,
        leasing_company = kpnwe_ev_stg.stg_lw_vehicles.leasing_company,
        leasing_contract_ref = kpnwe_ev_stg.stg_lw_vehicles.leasing_contract_ref,
        wltp_range = kpnwe_ev_stg.stg_lw_vehicles.wltp_range,
        connector_type = kpnwe_ev_stg.stg_lw_vehicles.connector_type,
        max_power_ac = kpnwe_ev_stg.stg_lw_vehicles.max_power_ac,
        max_power_dc = kpnwe_ev_stg.stg_lw_vehicles.max_power_dc
    FROM 
        kpnwe_ev_stg.stg_lw_vehicles
    WHERE 
        kpnwe_ev.dim_lw_vehicles.vehicle_key = kpnwe_ev_stg.stg_lw_vehicles.vehicle_key        
        AND 
        kpnwe_ev.dim_lw_vehicles.last_modified_date IS NOT DISTINCT FROM kpnwe_ev_stg.stg_lw_vehicles.last_modified_date                  -- tacking column
        AND 
        kpnwe_ev.dim_lw_vehicles.is_current = true;

    -- 3) Insert new SCD2 row when tacking column combination does NOT exist
    -- note: Adding "IS NOT DISTINCT" instead of '<>'. This is because NULL values can't be matched with NULL, results in new rows being created instead of updating existing row.
    INSERT INTO kpnwe_ev.dim_lw_vehicles
        (
            vehicle_key,
            company_id,
            company_brand,
            model,
            registration_number,
            tank_capacity,
            battery_capacity,
            initial_odometer,
            last_odometer,
            leasing_start_date,
            vehicle_date,
            vehicle_type,
            eu_registered,
            code,
            creation_date,
            last_modified_date,
            co2,
            power_supply,
            chassis_number,
            horse_power,
            target_consumption,
            leasing_company,
            leasing_contract_ref,
            wltp_range,
            connector_type,
            max_power_ac,
            max_power_dc,
            ingest_timestamp,
            valid_from,
            valid_to,
            is_current            
        )
    SELECT
        kpnwe_ev_stg.stg_lw_vehicles.vehicle_key,
        kpnwe_ev_stg.stg_lw_vehicles.company_id,
        kpnwe_ev_stg.stg_lw_vehicles.company_brand,
        kpnwe_ev_stg.stg_lw_vehicles.model,
        kpnwe_ev_stg.stg_lw_vehicles.registration_number,
        kpnwe_ev_stg.stg_lw_vehicles.tank_capacity,
        kpnwe_ev_stg.stg_lw_vehicles.battery_capacity,
        kpnwe_ev_stg.stg_lw_vehicles.initial_odometer,
        kpnwe_ev_stg.stg_lw_vehicles.last_odometer,
        kpnwe_ev_stg.stg_lw_vehicles.leasing_start_date,
        kpnwe_ev_stg.stg_lw_vehicles.vehicle_date,
        kpnwe_ev_stg.stg_lw_vehicles.vehicle_type,
        kpnwe_ev_stg.stg_lw_vehicles.eu_registered,
        kpnwe_ev_stg.stg_lw_vehicles.code,
        kpnwe_ev_stg.stg_lw_vehicles.creation_date,
        kpnwe_ev_stg.stg_lw_vehicles.last_modified_date,
        kpnwe_ev_stg.stg_lw_vehicles.co2,
        kpnwe_ev_stg.stg_lw_vehicles.power_supply,
        kpnwe_ev_stg.stg_lw_vehicles.chassis_number,
        kpnwe_ev_stg.stg_lw_vehicles.horse_power,
        kpnwe_ev_stg.stg_lw_vehicles.target_consumption,
        kpnwe_ev_stg.stg_lw_vehicles.leasing_company,
        kpnwe_ev_stg.stg_lw_vehicles.leasing_contract_ref,
        kpnwe_ev_stg.stg_lw_vehicles.wltp_range,
        kpnwe_ev_stg.stg_lw_vehicles.connector_type,
        kpnwe_ev_stg.stg_lw_vehicles.max_power_ac,
        kpnwe_ev_stg.stg_lw_vehicles.max_power_dc,
        kpnwe_ev_stg.stg_lw_vehicles.ingest_timestamp,
        -- (NWEREP-3044) Adding COALESCE to pick "creation_date" for newly created records as they will not have modified date in "valid_from"
        -- COALESCE(kpnwe_ev_stg.stg_lw_vehicles.last_modified_date, kpnwe_ev_stg.stg_lw_vehicles.creation_date),
        -- (NWEREP-3002) Cap: To keep valid_from date as 01/03/2026 for any records that have modified_date or creation_date before 01/03/2026.
        GREATEST(COALESCE(kpnwe_ev_stg.stg_lw_vehicles.last_modified_date, kpnwe_ev_stg.stg_lw_vehicles.creation_date), DATE '2026-03-01'),        
        '9999-12-31',
        true
    FROM 
        kpnwe_ev_stg.stg_lw_vehicles
    WHERE NOT EXISTS (
        SELECT 1
        FROM 
            kpnwe_ev.dim_lw_vehicles
        WHERE 
            kpnwe_ev.dim_lw_vehicles.vehicle_key IS NOT DISTINCT FROM kpnwe_ev_stg.stg_lw_vehicles.vehicle_key
            AND 
            kpnwe_ev.dim_lw_vehicles.last_modified_date IS NOT DISTINCT FROM kpnwe_ev_stg.stg_lw_vehicles.last_modified_date             -- tacking column
    );

    RAISE INFO 'Upsert finished: kpnwe_ev.dim_lw_vehicles';

END;
$$

