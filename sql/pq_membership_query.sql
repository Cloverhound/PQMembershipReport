-- PQ Membership Query v4

SET NOCOUNT ON;
SET ANSI_WARNINGS ON;

DECLARE	@query AS NVARCHAR(MAX),
		@attribute_list  AS NVARCHAR(MAX),
		@atts_join AS NVARCHAR(MAX)


-- Construct a join statement string with Precision Queue step expressions in order to
-- join a list of PQ steps to a list of agents with associated attributes
SELECT @atts_join = (
	SELECT DISTINCT ' OR ( pqs.PrecisionQueueStepID = ' +  CAST(pqs2.PrecisionQueueStepID AS VARCHAR) + ' AND ' +
        (
            SELECT 

				CASE WHEN Precision_Queue_Term.TermRelation = 1 THEN ' AND '
						WHEN Precision_Queue_Term.TermRelation = 2 THEN ' OR '
						ELSE ''
				END +
				
				CASE WHEN Precision_Queue_Term.ParenCount = 1 THEN '('
						ELSE ''
				END +

				'atts.' + QUOTENAME(Attribute.EnterpriseName) + ' ' +

				CASE  
					When Precision_Queue_Term.AttributeRelation = 0 Then 'Unknown'
					When Precision_Queue_Term.AttributeRelation = 1 Then '='
					When Precision_Queue_Term.AttributeRelation = 2 Then '!='
					When Precision_Queue_Term.AttributeRelation = 3 Then '<'
					When Precision_Queue_Term.AttributeRelation = 4 Then '<='
					When Precision_Queue_Term.AttributeRelation = 5 Then '>'
					When Precision_Queue_Term.AttributeRelation = 6 Then '>='
					-- the below are documented in API guide but I don't think actually used
					-- if used they won't work at all presently
					When Precision_Queue_Term.AttributeRelation = 7 Then 'between'
					When Precision_Queue_Term.AttributeRelation = 8 Then 'member'
					When Precision_Queue_Term.AttributeRelation = 9 Then 'agent has attribute'
					When Precision_Queue_Term.AttributeRelation = 10 Then 'agent does not have attribute'
					When Precision_Queue_Term.AttributeRelation = 11 Then 'not member'
					END +
						
				CASE
					WHEN ISNUMERIC(Precision_Queue_Term.Value1) = 1 THEN Precision_Queue_Term.Value1
					ELSE ' ''' + Precision_Queue_Term.Value1 + ''' '
				END +

				CASE 
					WHEN Precision_Queue_Term.ParenCount = -1 THEN ')'
					ELSE ''
				END 
			AS [text()]

            FROM Precision_Queue, Precision_Queue_Step, Precision_Queue_Term, Attribute 
			WHERE 
				    Precision_Queue.PrecisionQueueID = Precision_Queue_Step.PrecisionQueueID and
					Precision_Queue_Step.PrecisionQueueStepID = Precision_Queue_Term.PrecisionQueueStepID and
					Precision_Queue_Step.PrecisionQueueStepID = pqs2.PrecisionQueueStepID and
					Attribute.AttributeID = Precision_Queue_Term.AttributeID

					-- CUIC param to filter by PQS, 1 of 2
			    		-- Edit or remove this if running directly in SQL Studio
					AND Precision_Queue.PrecisionQueueID IN (:pqs)
      
			ORDER BY Precision_Queue.PrecisionQueueID,
						Precision_Queue_Step.PrecisionQueueStepID,
						Precision_Queue_Step.StepOrder,
						Precision_Queue_Term.TermOrder

		FOR XML PATH(''),TYPE).value('.','NVARCHAR(MAX)')
		+ ' )' AS [text()]

	FROM Precision_Queue , Precision_Queue_Step pqs2,Precision_Queue_Term,Attribute 
	WHERE 
		Precision_Queue.PrecisionQueueID = pqs2.PrecisionQueueID and
		pqs2.PrecisionQueueID = Precision_Queue_Term.PrecisionQueueID  and
		pqs2.PrecisionQueueStepID = Precision_Queue_Term.PrecisionQueueStepID and
		Attribute.AttributeID = Precision_Queue_Term.AttributeID
	  
	FOR XML PATH (''),TYPE).value('.','NVARCHAR(MAX)');


-- Build comma-delimited string list of attributes to
-- use when building a pivot table with agent attributes
SELECT @attribute_list = STUFF((
			SELECT ',' + QUOTENAME(att.EnterpriseName) 
			FROM Attribute att					
			GROUP BY att.EnterpriseName
			ORDER BY att.EnterpriseName
			FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)') 
		,1,1,'');

-- Construct the final query
set @query = N'INSERT INTO #temp_pq_table SELECT AgentName, AgentSkillTargetID, pq.PrecisionQueueID, pq.EnterpriseName QueueName ' 
			 + N', MIN(pqs.StepOrder) FirstStep ' +
			 N' FROM 
             (
                SELECT a.EnterpriseName AgentName, a.SkillTargetID AgentSkillTargetID, aatt.AttributeValue, att.EnterpriseName AttributeName
						FROM Agent a
						 INNER JOIN Agent_Attribute aatt
							ON a.SkillTargetID = aatt.SkillTargetID
						 INNER JOIN Attribute att
							ON att.AttributeID = aatt.AttributeID
						
            ) x
            pivot 
            (
                max(AttributeValue)
                for AttributeName in (' + @attribute_list + N')
            ) atts  INNER JOIN  Precision_Queue_Step pqs ON 1=0 ' + @atts_join + 
				N' INNER JOIN Precision_Queue pq ON pqs.PrecisionQueueID = pq.PrecisionQueueID 
				GROUP BY AgentName, AgentSkillTargetID, pq.PrecisionQueueID, pq.EnterpriseName ';


--select @query

IF OBJECT_ID('tempdb.dbo.#temp_pq_table', 'U') IS NOT NULL
  DROP TABLE #temp_pq_table;
CREATE TABLE #temp_pq_table(AgentName nvarchar(300), AgentSkillTargetID int, PrecisionQueueID int, QueueName nvarchar(300), FirstStep int );

-- Run the main query, which dumps the results in the temp table
exec sp_executesql @query



-- Query the results from the temp table and do our final joins
SELECT pqm.*, 
		CASE(art.AgentState) 
			WHEN 1 THEN 'Logged On'
			WHEN 2 THEN 'Not Ready'
			WHEN 3 THEN 'Ready'
			WHEN 4 THEN 'Talking'
			WHEN 5 THEN 'Work Not Ready'
			WHEN 6 THEN 'Work Ready'
			WHEN 7 THEN 'Busy Other'
			WHEN 8 THEN 'Reserved'
			ELSE 'Logged Out' 
		END AgentState,
		art.ReasonCode,
		art.AgentState AgentStateCode,
		'Step ' + CAST(FirstStep AS VARCHAR) FirstStepLabel,
		pqs.PrecisionQueueStepID,
		rules.PQ_Rules PrecisionQueueRules

FROM #temp_pq_table pqm
	INNER JOIN Precision_Queue pq ON pq.PrecisionQueueID = pqm.PrecisionQueueID
	INNER JOIN Precision_Queue_Step pqs
		ON pqs.PrecisionQueueID = pq.PrecisionQueueID
		AND pqm.FirstStep = pqs.StepOrder

	INNER JOIN Agent a ON pqm.AgentSkillTargetID = a.SkillTargetID
	LEFT JOIN Agent_Real_Time art on a.SkillTargetID = art.SkillTargetID

	-- join in the rule string for this precision queue step
	INNER JOIN (
		SELECT DISTINCT Precision_Queue.PrecisionQueueID, pqs2.PrecisionQueueStepID, pqs2.StepOrder Step,
		'' + (SELECT
					CASE WHEN Precision_Queue_Term.TermRelation = 1 THEN ' AND '
							WHEN Precision_Queue_Term.TermRelation = 2 THEN ' OR '
							ELSE '' END +
				
					CASE WHEN Precision_Queue_Term.ParenCount = 1 THEN '[ '
							ELSE '' END +
				
					'(' + Attribute.EnterpriseName + ' ' +
					
					CASE  
						When Precision_Queue_Term.AttributeRelation = 0 Then 'Unknown'
						When Precision_Queue_Term.AttributeRelation = 1 Then '=='
						When Precision_Queue_Term.AttributeRelation = 2 Then '!='
						When Precision_Queue_Term.AttributeRelation = 3 Then '<'
						When Precision_Queue_Term.AttributeRelation = 4 Then '<='
						When Precision_Queue_Term.AttributeRelation = 5 Then '>'
						When Precision_Queue_Term.AttributeRelation = 6 Then '>='
						When Precision_Queue_Term.AttributeRelation = 7 Then 'between'
						When Precision_Queue_Term.AttributeRelation = 8 Then 'member'
						When Precision_Queue_Term.AttributeRelation = 9 Then 'agent has attribute'
						When Precision_Queue_Term.AttributeRelation = 10 Then 'agent does not have attribute'
						When Precision_Queue_Term.AttributeRelation = 11 Then 'not member'
						END + 
					
					' ' + Precision_Queue_Term.Value1 + ')' +
					
					CASE 
						WHEN Precision_Queue_Term.ParenCount = -1 THEN ' ]'
						ELSE '' END
				
				AS [text()]

				FROM Precision_Queue, Precision_Queue_Step,Precision_Queue_Term,Attribute 
				WHERE 
						Precision_Queue.PrecisionQueueID = Precision_Queue_Step.PrecisionQueueID and
						Precision_Queue_Step.PrecisionQueueID = Precision_Queue_Term.PrecisionQueueID  and
						Precision_Queue_Step.PrecisionQueueStepID = Precision_Queue_Term.PrecisionQueueStepID and
						Precision_Queue_Step.PrecisionQueueStepID = pqs2.PrecisionQueueStepID and
						Attribute.AttributeID = Precision_Queue_Term.AttributeID
      
				ORDER BY Precision_Queue.PrecisionQueueID,
							Precision_Queue_Step.PrecisionQueueStepID,
							Precision_Queue_Step.StepOrder,
							Precision_Queue_Term.TermOrder

				FOR XML PATH(''),TYPE).value('.','NVARCHAR(MAX)')
			AS [PQ_Rules]

		FROM Precision_Queue , Precision_Queue_Step pqs2,Precision_Queue_Term,Attribute 
		WHERE 
		  Precision_Queue.PrecisionQueueID = pqs2.PrecisionQueueID and
		  pqs2.PrecisionQueueID = Precision_Queue_Term.PrecisionQueueID  and
		  pqs2.PrecisionQueueStepID = Precision_Queue_Term.PrecisionQueueStepID and
		  Attribute.AttributeID = Precision_Queue_Term.AttributeID

	) rules ON rules.PrecisionQueueStepID = pqs.PrecisionQueueStepID
	-- end join on precision queue rules string

	-- CUIC param to filter by PQS, 2 of 2
	-- Edit or remove this if running directly in SQL Studio
	WHERE pqm.PrecisionQueueID in (:pqs)

DROP TABLE #temp_pq_table
