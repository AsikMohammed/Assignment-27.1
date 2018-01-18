USE [DAVE_Lookups]
GO

/****** Object:  StoredProcedure [dbo].[sp_update_inboundtelephony_bgs_vdn_hiera]    Script Date: 11/30/2017 3:05:08 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_update_inboundtelephony_bgs_vdn_hiera]
	@BatchInstanceId UNIQUEIDENTIFIER
AS
BEGIN

	SET NOCOUNT ON;
	
	----------------------------------------------------------------------------------------
	-- Start of MERGE update for type 1 scd
	----------------------------------------------------------------------------------------

	MERGE [dbo].[InboundTelephony_BGS_VDN_Hiera] AS [TARGET]
		USING  (  
				SELECT
					[HieraSK] AS [BKHieraSK],
					[switchID] AS [SwitchSK],
					[DNIS] AS [VDN],
					ISNULL([CallSource].callsourceName,'Unknown') AS [CallSource],
					ISNULL([subType].callsubtypeDesc,'Unknown') AS [CallSubType],
					ISNULL([CallType].calltypeDesc,'Unknown') AS [CallType],
					ISNULL([CallGroup].CallGroupName,'Unknown') AS [CallGroup],
					ISNULL([CallGroup].CallGroupName,'Unknown') AS [SubPL],
					ISNULL([CallGroup].CallGroupName,'Unknown') AS [PL],
					'BGS' AS [BusinessUnit],
					CASE WHEN [CallSource].callsourcedesc = 'ICM' THEN 'N' ELSE 'Y' END[Transfer_Flag],
					[validFrom] AS [ValidFrom],
					[validUntil] AS [ValidTo],
					[dialledNumber] AS [BGS_VDN_Hiera_SpareAttribute01]

				FROM [TAMI_Reference_Data].[dbo].[MI_VDNMap] [VDN]
					 INNER JOIN [TAMI_Reference_Data].[dbo].[MI_tblSwitch] [Switch] ON
					 [Switch].[ID] = [VDN].[switchID]
					 INNER JOIN [TAMI_Reference_Data].[dbo].[MI_tblSystem] [System] ON
					 [System].[ID] = [Switch].[systemID]
					 LEFT JOIN [TAMI_Reference_Data].[dbo].[MI_tblCallSubType] [subType] ON
					 [VDN].callSubTypeId = [subType].id
					 LEFT JOIN [TAMI_Reference_Data].[dbo].[MI_tblCallType] [CallType] ON
					 CAST([VDN].callTypeAnnId as INT) = [CallType].id
					 LEFT JOIN [TAMI_Reference_Data].[dbo].[MI_tblCallGroup] [CallGroup] ON
					 [CallType].callGroupId = [CallGroup].CallGroupID
					 LEFT JOIN [TAMI_Reference_Data].[dbo].[MI_tblCallSource] [CallSource] ON
					 [VDN].[callSourceID] = [CallSource].ID
				WHERE [System].[systemName] = 'Aspect'

				) AS [SOURCE]
		ON ([TARGET].[BKHieraSK] = [SOURCE].[BKHieraSK])
		
		----------------------------------------------------------------------------------------
		-- This is where we check for changes in our scd columns
		----------------------------------------------------------------------------------------
		WHEN MATCHED  THEN 
			UPDATE SET  [TARGET].[CallSource] = [SOURCE].[CallSource],
						[TARGET].[CallSubType] = [SOURCE].[CallSubType],
						[TARGET].[CallType] = [SOURCE].[CallType],
						[TARGET].[CallGroup] = [SOURCE].[CallGroup],
						[TARGET].[SubPL] = [SOURCE].[SubPL],
						[TARGET].[PL] = [SOURCE].[PL],
						[TARGET].[BusinessUnit] = [SOURCE].[BusinessUnit],
						[TARGET].[Transfer_Flag] = [SOURCE].[Transfer_Flag],
						[TARGET].[ValidTo] = [SOURCE].[ValidTo],
					    [TARGET].[LastUpdated] = GETDATE()
			
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (
					[BKHieraSK],
					[SwitchSK],
					[VDN],
					[CallSource],
					[CallSubType],
					[CallType],
					[CallGroup],
					[SubPL],
					[PL],
					[BusinessUnit],
					[Transfer_Flag],
					[ValidFrom],
					[ValidTo],
					[Created],
					[LastUpdated]
				   )
			VALUES (
					[BKHieraSK],
					[SwitchSK],
					[VDN],
					[CallSource],
					[CallSubType],
					[CallType],
					[CallGroup],
					[SubPL],
					[PL],
					[BusinessUnit],
					[Transfer_Flag],
					[ValidFrom],
					[ValidTo],
					GETDATE(),
					GETDATE()
				   );

END

GO

