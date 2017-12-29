USE [DAVE_ODS_Genesys]
GO

/****** Object:  StoredProcedure [infomart].[sp_merge_d_queue_group]    Script Date: 11/30/2017 6:17:30 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [infomart].[sp_merge_d_queue_group]
	@BatchInstanceId UNIQUEIDENTIFIER
AS
BEGIN
	SET NOCOUNT ON;

	INSERT [infomart].[D_QUEUE_GROUP] 
		(  
			[BatchInstanceId],
			[RESOURCE_ID],
			[OPEN_DATE],
			[CLOSED_DATE],
			[CREATED_DATE],
			[SITE],
			[QUEUE_GROUP_KEY],
			[SWITCH_QUEUE],
			[QUEUE],
			[CALL_TYPE],
			[CALL_GROUP],
			[SUB_P_AND_L],
			[P_AND_L],
			[BUSINESS_UNIT],
			[SCORECARD_FLAG],
			[TRANSFER_FLAG],
			[CUSTOMER_FACING_FLAG],
			[StartDt],
			[EndDt],
			[CurrentFlag],
			[SourceExtractedTS]
		)
	SELECT     
			@BatchInstanceId,
			[RESOURCE_ID],
			[OPEN_DATE],
			[CLOSED_DATE],
			[CREATED_DATE],
			[SITE],
			[QUEUE_GROUP_KEY],
			[SWITCH_QUEUE],
			[QUEUE],
			[CALL_TYPE],
			[CALL_GROUP],
			[SUB_P_AND_L],
			[P_AND_L],
			[BUSINESS_UNIT],
			[SCORECARD_FLAG],
			[TRANSFER_FLAG],
			[CUSTOMER_FACING_FLAG],
		    GETDATE(),
			NULL,
		    'Y',
		    GETDATE()
	FROM
		(
		MERGE [infomart].[D_QUEUE_GROUP] AS [TARGET]
		USING	(
				SELECT	  
				
					[RESOURCE_ID],
					[OPEN_DATE],
					[CLOSED_DATE],
					[CREATED_DATE],
					[SITE],
					[QUEUE_GROUP_KEY],
					[SWITCH_QUEUE],
					[QUEUE],
					[CALL_TYPE],
					[CALL_GROUP],
					[SUB_P_AND_L],
					[P_AND_L],
					[BUSINESS_UNIT],
					[SCORECARD_FLAG],
					[TRANSFER_FLAG],
					[CUSTOMER_FACING_FLAG]
						
				FROM [DAVE_Landing_Genesys].[infomart].[D_QUEUE_GROUP]
				) AS [SOURCE]
		ON ([TARGET].[QUEUE_GROUP_KEY] = [SOURCE].[QUEUE_GROUP_KEY] 
			AND  [TARGET].[CurrentFlag] = 'Y')
		
		----------------------------------------------------------------------------------------
		-- This is where we check for changes in our scd columns
		----------------------------------------------------------------------------------------
		WHEN MATCHED AND ( 
							ISNULL([SOURCE].[RESOURCE_ID],'') <> ISNULL([TARGET].[RESOURCE_ID],'') OR
							ISNULL([SOURCE].[OPEN_DATE],'') <> ISNULL([TARGET].[OPEN_DATE],'') OR
							ISNULL([SOURCE].[CLOSED_DATE],'') <> ISNULL([TARGET].[CLOSED_DATE],'') OR
							ISNULL([SOURCE].[CREATED_DATE],'') <> ISNULL([TARGET].[CREATED_DATE],'') OR
							ISNULL([SOURCE].[SITE],'') <> ISNULL([TARGET].[SITE],'') OR
							ISNULL([SOURCE].[SWITCH_QUEUE],'') <> ISNULL([TARGET].[SWITCH_QUEUE],'') OR
							ISNULL([SOURCE].[QUEUE],'') <> ISNULL([TARGET].[QUEUE],'') OR
							ISNULL([SOURCE].[CALL_TYPE],'') <> ISNULL([TARGET].[CALL_TYPE],'') OR
							ISNULL([SOURCE].[CALL_GROUP],'') <> ISNULL([TARGET].[CALL_GROUP],'') OR
							ISNULL([SOURCE].[SUB_P_AND_L],'') <> ISNULL([TARGET].[SUB_P_AND_L],'') OR
							ISNULL([SOURCE].[P_AND_L],'') <> ISNULL([TARGET].[P_AND_L],'') OR
							ISNULL([SOURCE].[BUSINESS_UNIT],'') <> ISNULL([TARGET].[BUSINESS_UNIT],'') OR
							ISNULL([SOURCE].[SCORECARD_FLAG],'') <> ISNULL([TARGET].[SCORECARD_FLAG],'') OR
							ISNULL([SOURCE].[TRANSFER_FLAG],'') <> ISNULL([TARGET].[TRANSFER_FLAG],'') OR
							ISNULL([SOURCE].[CUSTOMER_FACING_FLAG],'') <> ISNULL([TARGET].[CUSTOMER_FACING_FLAG],'')

						) THEN
			UPDATE SET [CurrentFlag]='N', [EndDt] = GETDATE(), [BatchInstanceId] = @BatchInstanceId
			
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (
					[BatchInstanceId],
					[RESOURCE_ID],
					[OPEN_DATE],
					[CLOSED_DATE],
					[CREATED_DATE],
					[SITE],
					[QUEUE_GROUP_KEY],
					[SWITCH_QUEUE],
					[QUEUE],
					[CALL_TYPE],
					[CALL_GROUP],
					[SUB_P_AND_L],
					[P_AND_L],
					[BUSINESS_UNIT],
					[SCORECARD_FLAG],
					[TRANSFER_FLAG],
					[CUSTOMER_FACING_FLAG],
					[StartDt],
					[EndDt],
					[CurrentFlag],
					[SourceExtractedTS]
					)
			VALUES (
					@BatchInstanceId,
					[RESOURCE_ID],
					[OPEN_DATE],
					[CLOSED_DATE],
					[CREATED_DATE],
					[SITE],
					[QUEUE_GROUP_KEY],
					[SWITCH_QUEUE],
					[QUEUE],
					[CALL_TYPE],
					[CALL_GROUP],
					[SUB_P_AND_L],
					[P_AND_L],
					[BUSINESS_UNIT],
					[SCORECARD_FLAG],
					[TRANSFER_FLAG],
					[CUSTOMER_FACING_FLAG],
					GETDATE(),
					NULL,
					'Y',
					GETDATE()
					)
			OUTPUT $action,
					[SOURCE].[RESOURCE_ID],
					[SOURCE].[OPEN_DATE],
					[SOURCE].[CLOSED_DATE],
					[SOURCE].[CREATED_DATE],
					[SOURCE].[SITE],
					[SOURCE].[QUEUE_GROUP_KEY],
					[SOURCE].[SWITCH_QUEUE],
					[SOURCE].[QUEUE],
					[SOURCE].[CALL_TYPE],
					[SOURCE].[CALL_GROUP],
					[SOURCE].[SUB_P_AND_L],
					[SOURCE].[P_AND_L],
					[SOURCE].[BUSINESS_UNIT],
					[SOURCE].[SCORECARD_FLAG],
					[SOURCE].[TRANSFER_FLAG],
					[SOURCE].[CUSTOMER_FACING_FLAG]
		) AS OUTPUT (
					output_action,
					[RESOURCE_ID],
					[OPEN_DATE],
					[CLOSED_DATE],
					[CREATED_DATE],
					[SITE],
					[QUEUE_GROUP_KEY],
					[SWITCH_QUEUE],
					[QUEUE],
					[CALL_TYPE],
					[CALL_GROUP],
					[SUB_P_AND_L],
					[P_AND_L],
					[BUSINESS_UNIT],
					[SCORECARD_FLAG],
					[TRANSFER_FLAG],
					[CUSTOMER_FACING_FLAG]
					)
	WHERE output_action = 'UPDATE'

END
GO

