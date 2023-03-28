CREATE OR REPLACE PACKAGE BODY IRPT.IRPT_CR_ActualStaff
AS
   /****************************************************************************

            Package:  IRPT_CR_ActualStaff

            Modification Log:

            Date        SE      Description
            ------------------------------------------------------------

            01/19/2011  Ashfaq  Initial Creation

    ****************************************************************************/

    PROCEDURE process_ActualStaff (
        aParmDate   DATE
    )
    IS
        --Local variables
        lnloaddaily                     NUMBER := 0;
        lnbuildhistory                  NUMBER := 0;

        ldStartDate                     DATE;
        ldRundate                       DATE;
    BEGIN
        DBMS_OUTPUT.enable ( 1000000 );

        SELECT SYSDATE
             INTO ldStartDate
             FROM dual;
        DBMS_OUTPUT.put_line ('Time procedure process_ActualStaff Started :' || ldStartDate);

        ldRunDate := aParmDate - 1;
        DBMS_OUTPUT.put_line ( 'Parm Date: ' || aParmDate );
        DBMS_OUTPUT.put_line ( 'Run Date : ' || ldRunDate );

           irpt_dss_util.dsstrunc ( 'IRPT.CR_Advisor_VQ_Result' );
           COMMIT;

           Delete
             from irpt.CR_ActualStaff_Result nologging
            where Agg_Level = 'D';
           COMMIT;

           lnloaddaily := Load_Daily ( ldRunDate );
           SELECT SYSDATE
             INTO ldStartDate
             FROM DUAL;
           DBMS_OUTPUT.PUT_LINE ('function Load_Daily' || ldStartDate);

           lnbuildhistory := Build_History ( ldRunDate );
           SELECT SYSDATE
             INTO ldStartDate
             FROM DUAL;
           DBMS_OUTPUT.PUT_LINE ('function Build_History' || ldStartDate);

           SELECT SYSDATE
             INTO ldStartDate
             FROM dual;
           DBMS_OUTPUT.put_line ('Time procedure process_ActualStaff Ended :' || ldStartDate);

    END process_ActualStaff;

   /************************************************************************************************************

       Build Daily Records

   ************************************************************************************************************/
   FUNCTION Load_Daily  (aRunDate   DATE)
        RETURN NUMBER
    IS
        lnInserted                    NUMBER := 0;
        lnUpdated                     NUMBER := 0;
        perform_update                number := 0;

    cursor c_agtsthist_0 is
    select advisor_ID,
           sum(Allocated) WORK_GRAND_TOTAL
      from irpt.CR_ActualStaff_Result
     where Agg_Level = 'D'
     group by advisor_ID;

    cursor c_agtsthist_1 is
    select advisor_ID,
           UNALLOC_READY Ready
      from irpt.Inbound_Unallocated;

    cursor c_agtsthist_2 is
    select advisor_ID,
           Advisor_Name
      from irpt.work_advisor_info;

    cursor c_agtsthist_3 is
    select ADVISOR_ID,
           COUNT_INTERACT,
           COUNT_CASE,
           COUNT_BREAK,
           COUNT_COACHING,
           COUNT_MENTORING,
           COUNT_RESTROOM,
           COUNT_TEAM_MEETING,
           COUNT_ACCOUNT_RESEARCH,
           COUNT_TRAINING,
           COUNT_SPECIAL_PROJECTS,
           COUNT_LUNCH,
           COUNT_END_OF_SHIFT,
           COUNT_Lost_Emer
      from IRPT.InfoMart_Adv_Break_Count
     WHERE START_DATE >= ( aRunDate )
       and START_DATE <  ( aRunDate + 1 );

    cursor c_agtsthist_4 is
    select advisor_ID,
           line_of_business,
           count (*) short_call_cnt
      from irpt.INBOUND_CALL
     where short_call = 'Y'
       and call_count_today = 'Y'
       and annex_tab is not null
     group by advisor_ID, line_of_business;

    cursor c_agtsthist_5 is
    select advisor_ID,
           line_of_business,
           SUM(Duration_RONA) as RONA_Dur,
           count (*)          as RONA_Cnt
      from irpt.INBOUND_CALL
     where END_POINT = 'O'
       and call_count_today = 'Y'
       and annex_tab is not null
     group by advisor_ID, line_of_business;

    cursor c_agtsthist_6 is
    select Agent_ID             as Advisor_IEX_ID,
           SUBSTR(Login_ID,1,5) as Advisor_ACD
      from irpt.work_TTV_AgtACD;

    cursor c_agtsthist_7 is
    select advisor_ID,
           VQ_NAME,
           count (*) as RONA_Cnt
      from irpt.INBOUND_CALL
     where END_POINT = 'O'
       and call_count_today = 'Y'
       and annex_tab is not null
     group by advisor_ID, VQ_NAME;

     cursor c_agtsthist_8 is
     select Advisor_ID,
            unalloc_ready
      from IRPT.Inbound_Unallocated
     where advisor_ID not in ( select Advisor_ID from IRPT.CR_ActualStaff_Result
                                where offline_flag <> 'Y'
                                  and agg_level = 'D');

    BEGIN
        BEGIN
          insert into irpt.CR_ActualStaff_Result
           ( CALL_CENTER, START_DATE, END_DATE, ADVISOR_ID, ADVISOR_NAME, LINE_OF_BUSINESS,
             RONA_CNT, RONA_DURATION, OFFLINE_FLAG, NOT_READY,
             BREAK_CNT, BREAK, COACHING_CNT, COACHING, MENTORING_CNT, MENTORING, RESTROOM_CNT, RESTROOM, TEAM_MEETING_CNT, TEAM_MEETING,
             ACCOUNT_RESEARCH_CNT, ACCOUNT_RESEARCH, TRAINING_CNT, TRAINING, SPECIAL_PROJECTS_CNT, SPECIAL_PROJECTS, LUNCH_CNT, LUNCH,
             END_OF_SHIFT_CNT, END_OF_SHIFT,
             TALK_UNALLOCATED, CONSULT_UNALLOCATED, ACW_UNALLOCATED, VS_UNALLOCATED, INTERACT_CNT, INTERACT_UNALLOCATED, CASE_CNT, CASE_UNALLOCATED,
             READY, UNALLOCATED, STAFFTIME, HANDLETIME, FTE, work_Grand_TOTAL, Agg_Level, short_call_cnt, Lost_Emer_CNT, Lost_Emer_Unallocated,
             Advisor_ACD,
             INBOUND_COUNT, TALK_TIME, CONSULT_TIME, OUTBOUND_TIME, VS_DURATION, ACW, HOLD_TIME, ALLOCATED, RING_TIME, PSAP_TIME,
             CONSULT_TIME_CNT, OUTBOUND_TIME_CNT, HOLD_TIME_CNT, PSAP_TIME_CNT, VS_DURATION_CNT, ACW_CNT
           )
             ( select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
              0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
              'D',0,0,0,Advisor_ACD,
              sum(F1) INBOUND_COUNT, sum(F2) TALK_TIME, sum(F3) CONSULT_TIME, sum(F4) OUTBOUND_TIME, sum(F5) VS_DURATION, sum(F6) ACW, sum(F7) HOLD_TIME,
              sum(F8) ALLOCATED, sum(F9) RING_TIME, sum(F10) PSAP_TIME,
              sum(F11) CONSULT_TIME_CNT, sum(F12) OUTBOUND_TIME_CNT, sum(F13) HOLD_TIME_CNT, sum(F14) PSAP_TIME_CNT, sum(F15) VS_DURATION_CNT, sum(F16) ACW_CNT
         from
            ( select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     count (*) F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10, 0 F11, 0 F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, sum(Duration_CAL) F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10, 0 F11, 0 F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, sum(Duration_CST) F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10, 0 F11, 0 F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, SUM(Duration_Outbound) F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10, 0 F11, 0 F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4,  SUM(Duration_VS) F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10, 0 F11, 0 F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, SUM(Duration_ACW) F6, 0 F7, 0 F8, 0 F9, 0 F10, 0 F11, 0 F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, SUM(Duration_Hold) F7, 0 F8, 0 F9, 0 F10, 0 F11, 0 F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7,
                     sum(Duration_CAL) + sum(Duration_CST) + SUM(Duration_Outbound) + SUM(Duration_VS) + SUM(Duration_ACW) + SUM(Duration_Hold) + SUM(Duration_PSAP) F8,
                     0 F9, 0 F10, 0 F11, 0 F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, SUM(Duration_Ring) F9, 0 F10, 0 F11, 0 F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, SUM(Duration_PSAP) F10, 0 F11, 0 F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10, count (*) F11, 0 F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and DURATION_CST     > 0
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10, 0 F11, count (*) F12, 0 F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT         in ('AI', 'CC')
                 and call_count_today  = 'Y'
                 and DURATION_OUTBOUND > 0
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10, 0 F11, 0 F12, count (*) F13, 0 F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and DURATION_HOLD    > 0
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10, 0 F11, 0 F12, 0 F13, count (*) F14, 0 F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and DURATION_PSAP    > 0
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10, 0 F11, 0 F12, 0 F13, 0 F14, count (*) F15, 0 F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and DURATION_VS      > 0
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, advisor_name, line_of_business,
                     0,0,'N',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     'D',0,0,0,Advisor_ACD,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10, 0 F11, 0 F12, 0 F13, 0 F14, 0 F15, count (*) F16
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and DURATION_ACW     > 0
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
      )
      group by CALL_CENTER, START_DATE, advisor_ID, Advisor_ACD, advisor_name, line_of_business
      );
            lnInserted := SQL%ROWCOUNT;
            COMMIT;

        EXCEPTION
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for the insert into Load_Daily 1');
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
        END;

        update irpt.CR_ActualStaff_Result
        set CONSULT_TIME_CNT = 0
        where CONSULT_TIME_CNT  is null;
        commit;
        update irpt.CR_ActualStaff_Result
        set OUTBOUND_TIME_CNT = 0
        where OUTBOUND_TIME_CNT is null;
        commit;
        update irpt.CR_ActualStaff_Result
        set HOLD_TIME_CNT = 0
        where HOLD_TIME_CNT is null;
        commit;
        update irpt.CR_ActualStaff_Result
        set PSAP_TIME_CNT = 0
        where PSAP_TIME_CNT is null;
        commit;
        update irpt.CR_ActualStaff_Result
        set VS_DURATION_CNT = 0
        where VS_DURATION_CNT is null;
        commit;
        update irpt.CR_ActualStaff_Result
        set ACW_CNT = 0
        where ACW_CNT is null;
        commit;

        BEGIN
          insert into irpt.CR_Advisor_VQ_Result
           ( CALL_CENTER, START_DATE, END_DATE, ADVISOR_ID, ADVISOR_IEX_ID, ADVISOR_NAME, PRI_WORK_GRP, VQ_NAME,
             INBOUND_COUNT, Short_Call_cnt, Allocated, Talk_Time, Consult_Time, Outbound_Time, Hold_Time, PSAP_Time, VS_Duration, ACW
           )
             ( select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME,
                      sum(F1) INBOUND_COUNT, sum(F2) Short_Call_cnt, sum(F3) Allocated, sum(F4) Talk_Time, sum(F5) Consult_Time, sum(F6) Outbound_Time, sum(F7) Hold_Time,
                      sum(F8) PSAP_Time, sum(F9) VS_Duration, sum(F10) ACW
         from
            ( select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME,
                     count (*) F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10
                from irpt.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME,
                     0 F1, count (*) F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10
                from IRPT.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and annex_tab is not null
                 and short_call = 'Y'
               group by CALL_CENTER, START_DATE, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME,
                     0 F1, 0 F2, sum(Duration_CAL+Duration_CST+Duration_Outbound+Duration_VS+Duration_ACW+Duration_Hold+Duration_PSAP) F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10
                from IRPT.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME,
                     0 F1, 0 F2, 0 F3, sum(Duration_CAL) F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10
                from IRPT.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME,
                     0 F1, 0 F2, 0 F3, 0 F4, sum(Duration_CST) F5, 0 F6, 0 F7, 0 F8, 0 F9, 0 F10
                from IRPT.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, sum(Duration_Outbound) F6, 0 F7, 0 F8, 0 F9, 0 F10
                from IRPT.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, sum(Duration_Hold) F7, 0 F8, 0 F9, 0 F10
                from IRPT.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME
               UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, sum(Duration_PSAP) F8, 0 F9, 0 F10
                from IRPT.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME
                UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, sum(Duration_VS) F9, 0 F10
                from IRPT.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME
                UNION ALL
              select Substr(Advisor_ID,1,3) Call_Center, START_DATE, START_DATE+1, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME,
                     0 F1, 0 F2, 0 F3, 0 F4, 0 F5, 0 F6, 0 F7, 0 F8, 0 F9, sum(Duration_ACW) F10
                from IRPT.INBOUND_CALL
               where END_POINT        in ('AI', 'CC')
                 and call_count_today = 'Y'
                 and annex_tab is not null
               group by CALL_CENTER, START_DATE, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME
            )
         group by CALL_CENTER, START_DATE, advisor_ID, ADVISOR_IEX_ID, advisor_name, PRI_WORK_GRP, VQ_NAME
            );
            lnInserted := SQL%ROWCOUNT;
            COMMIT;

        EXCEPTION
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for the insert into Load_Daily 1A');
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
        END;

        update irpt.CR_Advisor_VQ_Result
        SET RONA_CNT = 0;
        commit;

        FOR lmagtsthist_0 IN c_agtsthist_0
        LOOP
        BEGIN
          UPDATE irpt.CR_ActualStaff_Result
             SET WORK_GRAND_TOTAL = lmagtsthist_0.WORK_GRAND_TOTAL
           WHERE Advisor_ID       = lmagtsthist_0.Advisor_ID
             AND Agg_Level = 'D';
          lnUpdated := SQL%ROWCOUNT;
          COMMIT;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              perform_update := 0;
            WHEN OTHERS THEN
              gnErrNum := SQLCODE;
              gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
              DBMS_OUTPUT.put_line ('TERMINATED: exception in Load_Daily 2');
              DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
              DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
              ROLLBACK;
              RETURN ( -1 );
        END;
        END LOOP;

        FOR lmagtsthist_1 IN c_agtsthist_1
        LOOP
        BEGIN
          UPDATE irpt.CR_ActualStaff_Result
             SET READY      = lmagtsthist_1.READY
           WHERE Advisor_ID = lmagtsthist_1.Advisor_ID
             AND Agg_Level  = 'D';
          lnUpdated := SQL%ROWCOUNT;
          COMMIT;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              perform_update := 0;
            WHEN OTHERS THEN
              gnErrNum := SQLCODE;
              gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
              DBMS_OUTPUT.put_line ('TERMINATED: exception in Load_Daily 3');
              DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
              DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
              ROLLBACK;
              RETURN ( -1 );
        END;
        END LOOP;

        UPDATE irpt.CR_ActualStaff_Result
           SET Ready = Ready * (Allocated / decode(WORK_GRAND_TOTAL,0,1,WORK_GRAND_TOTAL))
         WHERE Agg_Level = 'D';

    BEGIN
            insert into irpt.CR_ActualStaff_Result
                        ( CALL_CENTER,
                          START_DATE,
                          END_DATE,
                          ADVISOR_ID,
                          ADVISOR_NAME,
                          LINE_OF_BUSINESS,
                          INBOUND_COUNT,
                          RING_TIME,
                          TALK_TIME,
                          CONSULT_TIME,
                          Outbound_TIME,
                          HOLD_TIME,
                          PSAP_TIME,
                          VS_DURATION,
                          ACW,
                          RONA_CNT,
                          RONA_Duration,
                          OFFLINE_FLAG,
                          NOT_READY,
                          BREAK_CNT,
                          BREAK,
                          COACHING_CNT,
                          COACHING,
                          MENTORING_CNT,
                          MENTORING,
                          RESTROOM_CNT,
                          RESTROOM,
                          TEAM_MEETING_CNT,
                          TEAM_MEETING,
                          ACCOUNT_RESEARCH_CNT,
                          ACCOUNT_RESEARCH,
                          TRAINING_CNT,
                          TRAINING,
                          SPECIAL_PROJECTS_CNT,
                          SPECIAL_PROJECTS,
                          LUNCH_CNT,
                          LUNCH,
                          END_OF_SHIFT_CNT,
                          END_OF_SHIFT,
                          TALK_UNALLOCATED,
                          CONSULT_UNALLOCATED,
                          ACW_UNALLOCATED,
                          VS_UNALLOCATED,
                          INTERACT_CNT,
                          INTERACT_UNALLOCATED,
                          CASE_CNT,
                          CASE_UNALLOCATED,
                          READY,
                          ALLOCATED,
                          UNALLOCATED,
                          STAFFTIME,
                          HANDLETIME,
                          FTE,
                          work_Grand_TOTAL,
                          Agg_Level,
                          short_call_cnt,
                          Lost_Emer_CNT,
                          Lost_Emer_Unallocated,
                          Advisor_ACD,
                          CONSULT_TIME_CNT, OUTBOUND_TIME_CNT, HOLD_TIME_CNT, PSAP_TIME_CNT, VS_DURATION_CNT, ACW_CNT
                        )
                ( select distinct substr(a.advisor_ID,1,3) CALL_CENTER,
                         trunc(a.START_DATE),
                         trunc(a.START_DATE+1),
                         a.advisor_ID,
                         null,
                         a.Pri_Work_Grp,
                         0,0,0,0,0,0,0,0,0,0,0,
                         'Y',
                         a.UNALLOC_NR                Not_Ready,
                         0,
                         a.UNALLOC_BREAK             Break,
                         0,
                         a.UNALLOC_COACHING          Coaching,
                         0,
                         a.UNALLOC_MENTORING         MENTORING,
                         0,
                         a.UNALLOC_RESTROOM          RESTROOM,
                         0,
                         a.UNALLOC_TEAM_MEETING      TEAM_MEETING,
                         0,
                         a.UNALLOC_ACCOUNT_RESEARCH  ACCOUNT_RESEARCH,
                         0,
                         a.UNALLOC_TRAINING          TRAINING,
                         0,
                         a.UNALLOC_SPECIAL_PROJECTS  SPECIAL_PROJECTS,
                         0,
                         a.UNALLOC_LUNCH             LUNCH,
                         0,
                         a.UNALLOC_END_OF_SHIFT      END_OF_SHIFT,
                         a.UNALLOC_CAL               Talk_Unallocated,
                         a.UNALLOC_CST               Consult_Unallocated,
                         a.UNALLOC_ACW               ACW_Unallocated,
                         a.UNALLOC_VS                Unallocated,
                         0,
                         a.UNALLOC_INTERACT          Interact_Unallocated,
                         0,
                         a.UNALLOC_CASE              Case_Unallocated,
                         0,0,
                         a.UNALLOC_CAL + a.UNALLOC_CST      + a.UNALLOC_ACW +
                         a.UNALLOC_VS  + a.UNALLOC_INTERACT + a.UNALLOC_CASE UNALLOCATED,
                         0,0,0,0,
                         'D',
                         0, 0,
                         a.unalloc_Lost_Emer,
                         Advisor_ACD,
                         0,0,0,0,0,0
                    from irpt.Inbound_Unallocated a
                 );

            lnInserted := SQL%ROWCOUNT;
            COMMIT;

        EXCEPTION
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for the insert into Load_Daily 4');
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
        END;

        FOR lmagtsthist_2 IN c_agtsthist_2
        LOOP
        BEGIN
          UPDATE irpt.CR_ActualStaff_Result
             SET Advisor_Name = lmagtsthist_2.Advisor_Name
           WHERE Advisor_ID   = lmagtsthist_2.Advisor_ID
             AND OFFLINE_FLAG = 'Y'
             AND Agg_Level    = 'D';
          lnUpdated := SQL%ROWCOUNT;
          COMMIT;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              perform_update := 0;
            WHEN OTHERS THEN
              gnErrNum := SQLCODE;
              gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
              DBMS_OUTPUT.put_line ('TERMINATED: exception in Load_Daily 5');
              DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
              DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
              ROLLBACK;
              RETURN ( -1 );
        END;
        END LOOP;

        FOR lmagtsthist_3 IN c_agtsthist_3
        LOOP
        BEGIN
          UPDATE irpt.CR_ActualStaff_Result
             SET Interact_CNT         = lmagtsthist_3.COUNT_INTERACT,
                 Case_CNT             = lmagtsthist_3.COUNT_Case,
                 BREAK_CNT            = lmagtsthist_3.COUNT_BREAK,
                 COACHING_CNT         = lmagtsthist_3.COUNT_COACHING,
                 MENTORING_CNT        = lmagtsthist_3.COUNT_MENTORING,
                 RESTROOM_CNT         = lmagtsthist_3.COUNT_RESTROOM,
                 TEAM_MEETING_CNT     = lmagtsthist_3.COUNT_TEAM_MEETING,
                 ACCOUNT_RESEARCH_CNT = lmagtsthist_3.COUNT_ACCOUNT_RESEARCH,
                 TRAINING_CNT         = lmagtsthist_3.COUNT_TRAINING,
                 SPECIAL_PROJECTS_CNT = lmagtsthist_3.COUNT_SPECIAL_PROJECTS,
                 LUNCH_CNT            = lmagtsthist_3.COUNT_LUNCH,
                 END_OF_SHIFT_CNT     = lmagtsthist_3.COUNT_END_OF_SHIFT,
                 Lost_Emer_CNT        = lmagtsthist_3.COUNT_Lost_Emer
           WHERE Advisor_ID   = lmagtsthist_3.Advisor_ID
             AND OFFLINE_FLAG = 'Y'
             AND Agg_Level    = 'D';
          lnUpdated := SQL%ROWCOUNT;
          COMMIT;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              perform_update := 0;
            WHEN OTHERS THEN
              gnErrNum := SQLCODE;
              gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
              DBMS_OUTPUT.put_line ('TERMINATED: exception in Load_Daily 6');
              DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
              DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
              ROLLBACK;
              RETURN ( -1 );
        END;
        END LOOP;

        FOR lmagtsthist_4 IN c_agtsthist_4
        LOOP
        BEGIN
          UPDATE irpt.CR_ActualStaff_Result
             SET short_call_cnt   = lmagtsthist_4.short_call_cnt
           WHERE Advisor_ID       = lmagtsthist_4.Advisor_ID
             AND Line_OF_Business = lmagtsthist_4.Line_OF_Business
             AND OFFLINE_FLAG <> 'Y'
             AND Agg_Level    =  'D';
          lnUpdated := SQL%ROWCOUNT;
          COMMIT;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              perform_update := 0;
            WHEN OTHERS THEN
              gnErrNum := SQLCODE;
              gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
              DBMS_OUTPUT.put_line ('TERMINATED: exception in Load_Daily 7');
              DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
              DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
              ROLLBACK;
              RETURN ( -1 );
        END;
        END LOOP;

        FOR lmagtsthist_5 IN c_agtsthist_5
        LOOP
        BEGIN
          UPDATE irpt.CR_ActualStaff_Result
             SET RONA_Duration    = lmagtsthist_5.RONA_Dur,
                 RONA_CNT         = lmagtsthist_5.RONA_Cnt
           WHERE Advisor_ID       = lmagtsthist_5.Advisor_ID
             AND Line_OF_Business = lmagtsthist_5.Line_OF_Business
             AND OFFLINE_FLAG <> 'Y'
             AND Agg_Level    =  'D';
          lnUpdated := SQL%ROWCOUNT;
          COMMIT;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              perform_update := 0;
            WHEN OTHERS THEN
              gnErrNum := SQLCODE;
              gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
              DBMS_OUTPUT.put_line ('TERMINATED: exception in Load_Daily 8');
              DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
              DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
              ROLLBACK;
              RETURN ( -1 );
        END;
        END LOOP;

        FOR lmagtsthist_6 IN c_agtsthist_6
        LOOP
        BEGIN
          UPDATE irpt.CR_ActualStaff_Result
             SET Advisor_IEX_ID = lmagtsthist_6.Advisor_IEX_ID
           WHERE Advisor_ACD    = lmagtsthist_6.Advisor_ACD
             AND Agg_Level      = 'D';
          lnUpdated := SQL%ROWCOUNT;
          COMMIT;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              perform_update := 0;
            WHEN OTHERS THEN
              gnErrNum := SQLCODE;
              gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
              DBMS_OUTPUT.put_line ('TERMINATED: exception in Load_Daily 9');
              DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
              DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
              ROLLBACK;
              RETURN ( -1 );
        END;
        END LOOP;

        FOR lmagtsthist_7 IN c_agtsthist_7
        LOOP
        BEGIN
          UPDATE irpt.CR_Advisor_VQ_Result
             SET RONA_CNT         = lmagtsthist_7.RONA_Cnt
           WHERE Advisor_ID       = lmagtsthist_7.Advisor_ID
             AND VQ_NAME          = lmagtsthist_7.VQ_NAME;
          lnUpdated := SQL%ROWCOUNT;
          COMMIT;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              perform_update := 0;
            WHEN OTHERS THEN
              gnErrNum := SQLCODE;
              gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
              DBMS_OUTPUT.put_line ('TERMINATED: exception in Load_Daily 10');
              DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
              DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
              ROLLBACK;
              RETURN ( -1 );
        END;
        END LOOP;

        FOR lmagtsthist_8 IN c_agtsthist_8
        LOOP
        BEGIN
          UPDATE IRPT.CR_ActualStaff_Result
             SET READY      = lmagtsthist_8.unalloc_ready
           WHERE Advisor_ID = lmagtsthist_8.Advisor_ID
             AND Agg_Level  = 'D';
          lnUpdated := SQL%ROWCOUNT;
          COMMIT;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              perform_update := 0;
            WHEN OTHERS THEN
              gnErrNum := SQLCODE;
              gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
              DBMS_OUTPUT.put_line ('TERMINATED: exception in Load_Daily 11');
              DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
              DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
              ROLLBACK;
              RETURN ( -1 );
        END;
        END LOOP;

        UPDATE irpt.CR_ActualStaff_Result
           SET STAFFTIME = ALLOCATED      + TALK_UNALLOCATED     + CONSULT_UNALLOCATED + VS_UNALLOCATED +
                           ACW_UNALLOCATED + INTERACT_UNALLOCATED + LOST_EMER_UNALLOCATED + READY
         WHERE Agg_Level = 'D';

        UPDATE irpt.CR_ActualStaff_Result
           SET HANDLETIME = ALLOCATED      + TALK_UNALLOCATED     + CONSULT_UNALLOCATED + VS_UNALLOCATED +
                            ACW_UNALLOCATED + INTERACT_UNALLOCATED + LOST_EMER_UNALLOCATED
         WHERE Agg_Level  = 'D';

        UPDATE irpt.CR_ActualStaff_Result
           SET FTE       = (ALLOCATED      + TALK_UNALLOCATED     + CONSULT_UNALLOCATED + VS_UNALLOCATED +
                            ACW_UNALLOCATED + INTERACT_UNALLOCATED + CASE_UNALLOCATED + LOST_EMER_UNALLOCATED + READY) / (60*60*8)
         WHERE Agg_Level = 'D';

        UPDATE irpt.CR_ActualStaff_Result
          SET OFFLINE_FLAG = ' '
        WHERE OFFLINE_FLAG = 'N'
          AND Agg_Level    = 'D';
        commit;

    RETURN lnInserted;
    END Load_Daily;

    /************************************************************************************************************

       Delete from Data Mart Tables if it there is a Re-Run and Build History for Weekly and Monthly

    ************************************************************************************************************/

    FUNCTION Build_History (aRunDate   DATE)
        RETURN NUMBER
    IS
        lnDeleted   NUMBER := 0;
    BEGIN

        BEGIN
            DELETE
            FROM  irpt.CR_ActualStaff_Result
            WHERE Start_Date >= ( aRunDate )
              and Start_Date <  ( aRunDate + 1 )
              and Agg_Level = 'H';

            lnDeleted := SQL%ROWCOUNT;
            COMMIT;
          EXCEPTION
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for delete from Build_History 1' );
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
        END;

        BEGIN
            DELETE
            FROM  irpt.CR_ActualStaff_Result
            WHERE Start_Date < ( aRunDate - 45 )
              and Agg_Level = 'H';

            lnDeleted := SQL%ROWCOUNT;
            COMMIT;
          EXCEPTION
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for delete from Build_History 2' );
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
        END;

        insert into irpt.CR_ActualStaff_Result
        (Agg_Level, START_DATE, END_DATE, Advisor_ID, ADVISOR_NAME, Advisor_ACD, Advisor_IEX_ID, CALL_CENTER, LINE_OF_BUSINESS,
         Offline_Flag,
         INBOUND_COUNT, Short_call_cnt, RING_TIME, TALK_TIME, CONSULT_TIME, OUTBOUND_TIME, HOLD_TIME, PSAP_TIME, VS_DURATION,
         ACW, Not_Ready,
         Break_CNT, Break, Coaching_CNT, Coaching, MENTORING_CNT, MENTORING, RESTROOM_CNT, RESTROOM,
         TEAM_MEETING_CNT, TEAM_MEETING, ACCOUNT_RESEARCH_CNT, ACCOUNT_RESEARCH, TRAINING_CNT, TRAINING,
         SPECIAL_PROJECTS_CNT, SPECIAL_PROJECTS, LUNCH_CNT, LUNCH, END_OF_SHIFT_CNT, END_OF_SHIFT,
         Talk_Unallocated, Consult_Unallocated, VS_Unallocated, ACW_Unallocated, Lost_Emer_CNT, Lost_Emer_Unallocated,
         Interact_CNT, Interact_Unallocated, Case_CNT, Case_Unallocated, RONA_CNT, RONA_DURATION, Ready,
         ALLOCATED, UNALLOCATED, STAFFTIME, HANDLETIME,
         CONSULT_TIME_CNT, OUTBOUND_TIME_CNT, HOLD_TIME_CNT, PSAP_TIME_CNT, VS_DURATION_CNT, ACW_CNT
        )
        (select 'H', START_DATE, END_DATE, Advisor_ID, ADVISOR_NAME, Advisor_ACD, Advisor_IEX_ID, CALL_CENTER, LINE_OF_BUSINESS,
                Offline_Flag,
                INBOUND_COUNT, Short_call_cnt, RING_TIME, TALK_TIME, CONSULT_TIME, OUTBOUND_TIME, HOLD_TIME, PSAP_TIME, VS_DURATION,
                ACW, Not_Ready,
                Break_CNT, Break, Coaching_CNT, Coaching, MENTORING_CNT, MENTORING, RESTROOM_CNT, RESTROOM,
                TEAM_MEETING_CNT, TEAM_MEETING, ACCOUNT_RESEARCH_CNT, ACCOUNT_RESEARCH, TRAINING_CNT, TRAINING,
                SPECIAL_PROJECTS_CNT, SPECIAL_PROJECTS, LUNCH_CNT, LUNCH, END_OF_SHIFT_CNT, END_OF_SHIFT,
                Talk_Unallocated, Consult_Unallocated, VS_Unallocated, ACW_Unallocated, Lost_Emer_CNT, Lost_Emer_Unallocated,
                Interact_CNT, Interact_Unallocated, Case_CNT, Case_Unallocated, RONA_CNT, RONA_DURATION, Ready,
                ALLOCATED, UNALLOCATED, STAFFTIME, HANDLETIME,
                CONSULT_TIME_CNT, OUTBOUND_TIME_CNT, HOLD_TIME_CNT, PSAP_TIME_CNT, VS_DURATION_CNT, ACW_CNT
           from irpt.CR_ActualStaff_Result
          where Agg_Level = 'D'
         );
         commit;

        RETURN lnDeleted;
    END Build_History;

    /****************************************************************************

        Procedure: Process_weekly

    ****************************************************************************/

    PROCEDURE process_weekly (
        aParmDate   DATE
    )
    IS
        --Local variables

        lnload_weekly          NUMBER := 0;
        ldStartDate            DATE;
        ldRundate              DATE;

    BEGIN
        DBMS_OUTPUT.enable ( 1000000 );
        -- Write starting message
        SELECT SYSDATE
          INTO ldStartDate
          FROM DUAL;
        DBMS_OUTPUT.PUT_LINE ('Time procedure process_weekly Started: ' || ldStartDate);
        ldRunDate := aParmDate - 1;
        DBMS_OUTPUT.put_line ( 'Parm Date: ' || aParmDate );
        DBMS_OUTPUT.put_line ( 'Run Date : ' || ldRunDate );

        lnload_weekly := load_weekly ( ldRunDate );
        SELECT SYSDATE
          INTO ldStartDate
          FROM dual;
        DBMS_OUTPUT.put_line ('load_weekly is :' || ldStartDate);

        SELECT SYSDATE
          INTO ldStartDate
          FROM dual;
        DBMS_OUTPUT.put_line ('Time procedure process_weekly Ended:' || ldStartDate);

    END process_weekly;

   /********************************************************************************************************

         Load weekly data Agg_level "W" by summing the daily

    ************************************************************************************************************/
    FUNCTION LOAD_WEEKLY (aRunDate DATE)
        RETURN NUMBER
    IS
        lnDeleted       NUMBER := 0;
        lnInserted      NUMBER := 0;

    BEGIN

        BEGIN
            DELETE
            FROM  irpt.CR_ActualStaff_Result nologging
            WHERE AGG_LEVEL = 'W';

            lnDeleted := SQL%ROWCOUNT;
            COMMIT;
          EXCEPTION
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for delete from LOAD_WEEKLY 1' );
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
        END;

        BEGIN

          insert into irpt.CR_ActualStaff_Result
          (Agg_Level, START_DATE, END_DATE, Advisor_ID, ADVISOR_NAME, Advisor_ACD, Advisor_IEX_ID, CALL_CENTER, LINE_OF_BUSINESS,
           Offline_Flag,
           INBOUND_COUNT, Short_call_cnt, RING_TIME, TALK_TIME, CONSULT_TIME, OUTBOUND_TIME, HOLD_TIME, PSAP_TIME,
           VS_DURATION, ACW, Not_Ready,
           Break_CNT, Break, Coaching_CNT, Coaching, MENTORING_CNT, MENTORING, RESTROOM_CNT, RESTROOM,
           TEAM_MEETING_CNT, TEAM_MEETING, ACCOUNT_RESEARCH_CNT, ACCOUNT_RESEARCH, TRAINING_CNT, TRAINING,
           SPECIAL_PROJECTS_CNT, SPECIAL_PROJECTS, LUNCH_CNT, LUNCH, END_OF_SHIFT_CNT, END_OF_SHIFT,
           Talk_Unallocated, Consult_Unallocated, VS_Unallocated, ACW_Unallocated, Lost_Emer_CNT, Lost_Emer_Unallocated,
           Interact_CNT, Interact_Unallocated, Case_CNT, Case_Unallocated, RONA_CNT, RONA_DURATION, Ready,
           ALLOCATED, UNALLOCATED, STAFFTIME, HANDLETIME,
           CONSULT_TIME_CNT, OUTBOUND_TIME_CNT, HOLD_TIME_CNT, PSAP_TIME_CNT, VS_DURATION_CNT, ACW_CNT
          )
          (select 'W', (aRunDate - 6), (aRunDate), Advisor_ID, ADVISOR_NAME, Advisor_ACD, Advisor_IEX_ID, CALL_CENTER, LINE_OF_BUSINESS,
                  Offline_Flag,
                  sum(INBOUND_COUNT), sum(Short_call_cnt), sum(RING_TIME), sum(TALK_TIME), sum(CONSULT_TIME), sum(OUTBOUND_TIME), sum(HOLD_TIME), sum(PSAP_TIME),
                  sum(VS_DURATION), sum(ACW), sum(Not_Ready),
                  sum(Break_CNT), sum(Break), sum(Coaching_CNT), sum(Coaching), sum(MENTORING_CNT), sum(MENTORING), sum(RESTROOM_CNT), sum(RESTROOM),
                  sum(TEAM_MEETING_CNT), sum(TEAM_MEETING), sum(ACCOUNT_RESEARCH_CNT), sum(ACCOUNT_RESEARCH), sum(TRAINING_CNT), sum(TRAINING),
                  sum(SPECIAL_PROJECTS_CNT), sum(SPECIAL_PROJECTS), sum(LUNCH_CNT), sum(LUNCH), sum(END_OF_SHIFT_CNT), sum(END_OF_SHIFT),
                  sum(Talk_Unallocated), sum(Consult_Unallocated), sum(VS_Unallocated), sum(ACW_Unallocated), sum(Lost_Emer_CNT), sum(Lost_Emer_Unallocated),
                  sum(Interact_CNT), sum(Interact_Unallocated), sum(Case_CNT), sum(Case_Unallocated), sum(RONA_CNT), sum(RONA_DURATION), sum(Ready),
                  sum(ALLOCATED), sum(UNALLOCATED), sum(STAFFTIME), sum(HANDLETIME),
                  sum(CONSULT_TIME_CNT), sum(OUTBOUND_TIME_CNT), sum(HOLD_TIME_CNT), sum(PSAP_TIME_CNT), sum(VS_DURATION_CNT), sum(ACW_CNT)
             from irpt.CR_ActualStaff_Result
            where start_date >= (aRunDate - 6)
              AND start_date <= (aRunDate + 1)
              AND Agg_Level   = 'H'
            GROUP BY (aRunDate - 6), (aRunDate + 1), Advisor_ID, ADVISOR_NAME, Advisor_ACD, Advisor_IEX_ID, CALL_CENTER, LINE_OF_BUSINESS, Offline_Flag
           );
        lnInserted := SQL%ROWCOUNT;
            COMMIT;
        EXCEPTION
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for delete from LOAD_WEEKLY 2');
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
      END;

      UPDATE irpt.CR_ActualStaff_Result
           SET FTE       = (ALLOCATED      + TALK_UNALLOCATED     + CONSULT_UNALLOCATED + VS_UNALLOCATED +
                            ACW_UNALLOCATED + INTERACT_UNALLOCATED + CASE_UNALLOCATED + LOST_EMER_UNALLOCATED + READY) / (60*60*40)
         WHERE Agg_Level = 'W';

      RETURN lnInserted;
    END LOAD_WEEKLY;

    /****************************************************************************

            Load monthly data Agg_level "M" by summing the daily

    ****************************************************************************/

    PROCEDURE Process_monthly (
        aParmDate   DATE
    )
    IS
        --Local variables

        lnload_monthly         NUMBER := 0;
        lnxfer_dashboard       NUMBER := 0;
        lnfirst_month          DATE;
        lnlast_month           DATE;
        ldStartDate            DATE;
        ldRundate              DATE;
        lnfday_ofmonth         DATE;
        lnlday_ofmonth         DATE;

    BEGIN
        DBMS_OUTPUT.enable ( 1000000 );
        -- Write starting message
        SELECT SYSDATE
          INTO ldStartDate
          FROM DUAL;
        DBMS_OUTPUT.PUT_LINE ('Time the monthly osds_actual_staff STARTED: ' || ldStartDate);
        ldRunDate     := aParmDate - 1;
        lnfirst_month := aParmDate - 1;
        lnlast_month  := aParmDate - 1;
        DBMS_OUTPUT.put_line ( 'Parm Date: ' || aParmDate );
        DBMS_OUTPUT.put_line ( 'Run Date : ' || ldRunDate );

        irpt_dss_util.dsstrunc ( 'irpt.CR_Xfer_Dashboard' );
           COMMIT;

        lnfday_ofmonth := fday_ofmonth ( lnfirst_month );
        SELECT SYSDATE
          INTO ldStartDate
          FROM dual;
        DBMS_OUTPUT.put_line ('load_first_monthly is :' || ldStartDate);
        DBMS_OUTPUT.PUT_LINE ('lnfday_ofmonth: ' || lnfday_ofmonth);

        lnlday_ofmonth := lday_ofmonth ( lnlast_month );
        SELECT SYSDATE
          INTO ldStartDate
          FROM dual;
        DBMS_OUTPUT.put_line ('load_last_monthly is :' || ldStartDate);
        DBMS_OUTPUT.PUT_LINE ('lnlday_ofmonth: ' || lnlday_ofmonth);

        lnload_monthly := load_monthly ( lnfday_ofmonth, lnlday_ofmonth );
        SELECT SYSDATE
          INTO ldStartDate
          FROM dual;
        DBMS_OUTPUT.put_line ('load_monthly is :' || ldStartDate);

        lnxfer_dashboard := xfer_dashboard ( lnfday_ofmonth, lnlday_ofmonth );
        SELECT SYSDATE
          INTO ldStartDate
          FROM dual;
        DBMS_OUTPUT.put_line ('xfer_dashboard is :' || ldStartDate);

        SELECT SYSDATE
          INTO ldStartDate
          FROM dual;
        DBMS_OUTPUT.put_line ('Time the monthly job ended OSDS_ACTUAL_STAFF is :' || ldStartDate);

    END Process_monthly;

    FUNCTION fday_ofmonth(lnfirst_month DATE)
       RETURN DATE
    IS
       vMo VARCHAR2(3);
       vYr VARCHAR2(4);
       vDy VARCHAR2(2);

    BEGIN
       vMo := TO_CHAR(lnfirst_month, 'MON');
       vYr := TO_CHAR(lnfirst_month, 'YYYY');
       SELECT to_char(LAST_DAY(sysdate),'DD')
         INTO vDy
         FROM dual;
       RETURN TO_DATE('01-' || vMo || vYr, 'DD-MON-YYYY');

       EXCEPTION
         WHEN OTHERS THEN
           RETURN TO_DATE('01-JAN-1900', 'DD-MON-YYYY');

    END fday_ofmonth;

    FUNCTION lday_ofmonth(lnlast_month DATE)
       RETURN DATE
    IS
       vMo VARCHAR2(3);
       vYr VARCHAR2(4);
       vDy VARCHAR2(2);

    BEGIN
       vMo := TO_CHAR(lnlast_month, 'MON');
       vYr := TO_CHAR(lnlast_month, 'YYYY');
       SELECT to_char(LAST_DAY(lnlast_month),'DD')
         INTO vDy
         FROM dual;
       RETURN TO_DATE(vDy || vMo || vYr, 'DD-MON-YYYY');

       EXCEPTION
         WHEN OTHERS THEN
           RETURN TO_DATE('01-JAN-1900', 'DD-MON-YYYY');

     END lday_ofmonth;

   /************************************************************************************************************

         Summarize Monthly Data

    ************************************************************************************************************/
    FUNCTION LOAD_monthly (alnfday_ofmonth DATE, alnlday_ofmonth DATE)
        RETURN NUMBER
    IS
        lnDeleted       NUMBER := 0;
        lnInsertRec     NUMBER := 0;
        lnUpdated       NUMBER := 0;

    BEGIN

        BEGIN
            DELETE
            FROM  irpt.CR_ActualStaff_Result
            WHERE AGG_LEVEL = 'M';

            lnDeleted := SQL%ROWCOUNT;
            COMMIT;
          EXCEPTION
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for delete from LOAD_monthly 1' );
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
        END;

        BEGIN
          insert into irpt.CR_ActualStaff_Result
          (Agg_Level, START_DATE, END_DATE, Advisor_ID, ADVISOR_NAME, Advisor_ACD, Advisor_IEX_ID, CALL_CENTER, LINE_OF_BUSINESS,
           Offline_Flag,
           INBOUND_COUNT, Short_call_cnt, RING_TIME, TALK_TIME, CONSULT_TIME, OUTBOUND_TIME, HOLD_TIME, PSAP_TIME,
           VS_DURATION, ACW, Not_Ready,
           Break_CNT, Break, Coaching_CNT, Coaching, MENTORING_CNT, MENTORING, RESTROOM_CNT, RESTROOM,
           TEAM_MEETING_CNT, TEAM_MEETING, ACCOUNT_RESEARCH_CNT, ACCOUNT_RESEARCH, TRAINING_CNT, TRAINING,
           SPECIAL_PROJECTS_CNT, SPECIAL_PROJECTS, LUNCH_CNT, LUNCH, END_OF_SHIFT_CNT, END_OF_SHIFT,
           Talk_Unallocated, Consult_Unallocated, VS_Unallocated, ACW_Unallocated, Lost_Emer_CNT, Lost_Emer_Unallocated,
           Interact_CNT, Interact_Unallocated, Case_CNT, Case_Unallocated, RONA_CNT, RONA_DURATION, Ready,
           ALLOCATED, UNALLOCATED, STAFFTIME, HANDLETIME,
           CONSULT_TIME_CNT, OUTBOUND_TIME_CNT, HOLD_TIME_CNT, PSAP_TIME_CNT, VS_DURATION_CNT, ACW_CNT
          )
          (select 'M', alnfday_ofmonth, alnlday_ofmonth, Advisor_ID, ADVISOR_NAME, Advisor_ACD, Advisor_IEX_ID, CALL_CENTER, LINE_OF_BUSINESS,
                  Offline_Flag,
                  sum(INBOUND_COUNT), sum(Short_call_cnt), sum(RING_TIME), sum(TALK_TIME), sum(CONSULT_TIME), sum(OUTBOUND_TIME), sum(HOLD_TIME), sum(PSAP_TIME),
                  sum(VS_DURATION), sum(ACW), sum(Not_Ready),
                  sum(Break_CNT), sum(Break), sum(Coaching_CNT), sum(Coaching), sum(MENTORING_CNT), sum(MENTORING), sum(RESTROOM_CNT), sum(RESTROOM),
                  sum(TEAM_MEETING_CNT), sum(TEAM_MEETING), sum(ACCOUNT_RESEARCH_CNT), sum(ACCOUNT_RESEARCH), sum(TRAINING_CNT), sum(TRAINING),
                  sum(SPECIAL_PROJECTS_CNT), sum(SPECIAL_PROJECTS), sum(LUNCH_CNT), sum(LUNCH), sum(END_OF_SHIFT_CNT), sum(END_OF_SHIFT),
                  sum(Talk_Unallocated), sum(Consult_Unallocated), sum(VS_Unallocated), sum(ACW_Unallocated), sum(Lost_Emer_CNT), sum(Lost_Emer_Unallocated),
                  sum(Interact_CNT), sum(Interact_Unallocated), sum(Case_CNT), sum(Case_Unallocated), sum(RONA_CNT), sum(RONA_DURATION), sum(Ready),
                  sum(ALLOCATED), sum(UNALLOCATED), sum(STAFFTIME), sum(HANDLETIME),
                  sum(CONSULT_TIME_CNT), sum(OUTBOUND_TIME_CNT), sum(HOLD_TIME_CNT), sum(PSAP_TIME_CNT), sum(VS_DURATION_CNT), sum(ACW_CNT)
             from irpt.CR_ActualStaff_Result
            where start_date >= (alnfday_ofmonth)
              AND start_date <= (alnlday_ofmonth)
              AND Agg_Level   = 'H'
            GROUP BY alnfday_ofmonth, alnlday_ofmonth, Advisor_ID, ADVISOR_NAME, Advisor_ACD, Advisor_IEX_ID, CALL_CENTER, LINE_OF_BUSINESS, Offline_Flag
           );
            lnInsertRec := SQL%ROWCOUNT;
            COMMIT;
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                DBMS_OUTPUT.put_line ( 'No records in raw data table' );
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for insert LOAD_monthly 2' );
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
      END;

      UPDATE irpt.CR_ActualStaff_Result
           SET FTE       = (ALLOCATED      + TALK_UNALLOCATED     + CONSULT_UNALLOCATED + VS_UNALLOCATED +
                            ACW_UNALLOCATED + INTERACT_UNALLOCATED + CASE_UNALLOCATED + LOST_EMER_UNALLOCATED + READY) / (60*60*160)
         WHERE Agg_Level = 'M';

      RETURN lnInsertRec;
  END LOAD_monthly;

   /************************************************************************************************************

         Transfer DashBoard

    ************************************************************************************************************/
    FUNCTION Xfer_Dashboard (alnfday_ofmonth DATE, alnlday_ofmonth DATE)
        RETURN NUMBER
    IS
        lnDeleted       NUMBER := 0;
        lnInsertRec     NUMBER := 0;
        lnUpdated       NUMBER := 0;
        perform_update  number := 0;

    cursor c_dashboard_1 is
    select substr(From_Advisor_ID,1,3) Loc, From_Line_of_Business, count (*) Transferred
      from irpt.infomart_xfer
     where run_date >= (alnfday_ofmonth)
       AND run_date <= (alnlday_ofmonth)
       and substr( from_advisor_ID,1,3) in (select Site_Code from irpt.work_site)
       and From_Line_of_Business not like '%IVR%'
       and From_Line_of_Business not in ('XM','Default','Case_Work')
       and To_Line_of_Business   not like '%IVR%'
       and To_Line_of_Business   not in ('XM','Default')
       and To_VQ_Name <> 'EMD'
       and Flag_Conn = 1
       and Disposition in ('Consult', 'Consult + Transfer','Answered')
     group by substr(From_Advisor_ID,1,3), From_Line_of_Business;

    cursor c_dashboard_2 is
    select substr(From_Advisor_ID,1,3) Loc, From_Advisor_ID Advisor_ID, count (*) Transferred
      from irpt.infomart_xfer
     where run_date >= (alnfday_ofmonth)
       AND run_date <= (alnlday_ofmonth)
      and substr( from_advisor_ID,1,3) in (select Site_Code from irpt.work_site)
      and From_Line_of_Business not like '%IVR%'
      and From_Line_of_Business not in ('XM','Default','Case_Work')
      and To_Line_of_Business not like '%IVR%'
      and To_Line_of_Business not in ('XM','Default')
      and To_VQ_Name <> 'EMD'
      and Flag_Conn = 1
      and Disposition in ('Consult', 'Consult + Transfer','Answered')
    group by substr(From_Advisor_ID,1,3), From_Advisor_ID;

    BEGIN

      BEGIN
          insert into irpt.CR_Xfer_Dashboard
          ( SORT_ORDER,
            LOCATION,
            FROM_LINE_OF_BUSINESS,
            TO_LINE_OF_BUSINESS,
            ANSWERED,
            TRANSFERRED
          )
          ( select 1 Sort_Order, substr(Advisor_ID,1,3) Loc, Line_of_Business From_Line_of_Business, null, sum(INBOUND_COUNT) Answered, 0
              from irpt.CR_ActualStaff_Result
             where start_date >= (alnfday_ofmonth)
               AND start_date <= (alnlday_ofmonth)
               and Agg_Level   = 'H'
               and substr( advisor_ID,1,3) in (select Site_Code from irpt.work_site)
               and Line_of_Business not like '%IVR%'
               and Line_of_Business not in ('XM','Default')
             group by substr(Advisor_ID,1,3), Line_of_Business
            having sum(INBOUND_COUNT) > 0
           );
            lnInsertRec := SQL%ROWCOUNT;
            COMMIT;
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                DBMS_OUTPUT.put_line ( 'No records in raw data table' );
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for insert Xfer_Dashboard 1' );
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
      END;

      FOR lmdashboard_1 IN c_dashboard_1
        LOOP
        BEGIN
          UPDATE irpt.CR_Xfer_Dashboard
             SET Transferred = lmdashboard_1.Transferred
           WHERE Location              = lmdashboard_1.Loc
             AND From_Line_OF_Business = lmdashboard_1.From_Line_OF_Business;
          lnUpdated := SQL%ROWCOUNT;
          COMMIT;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              perform_update := 0;
            WHEN OTHERS THEN
              gnErrNum := SQLCODE;
              gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
              DBMS_OUTPUT.put_line ( 'TERMINATED: exception for insert Xfer_Dashboard 2');
              DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
              DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
              ROLLBACK;
              RETURN ( -1 );
        END;
        END LOOP;

      BEGIN
          insert into irpt.CR_Xfer_Dashboard
          ( SORT_ORDER,
            LOCATION,
            FROM_LINE_OF_BUSINESS,
            TO_LINE_OF_BUSINESS,
            ANSWERED,
            TRANSFERRED
          )
          ( select 1 Sort_Order, substr(From_Advisor_ID,1,3) Loc, From_Line_of_Business, To_Line_of_Business, null, count (*) Transferred
              from irpt.infomart_xfer
             where run_date >= (alnfday_ofmonth)
               AND run_date <= (alnlday_ofmonth)
               and substr( from_advisor_ID,1,3) in (select Site_Code from irpt.work_site)
               and From_Line_of_Business not like '%IVR%'
               and From_Line_of_Business not in ('XM','Default','Case_Work')
               and To_Line_of_Business not like '%IVR%'
               and To_Line_of_Business not in ('XM','Default')
               and Flag_Conn = 1
               and Disposition in ('Consult', 'Consult + Transfer','Answered')
             group by substr(From_Advisor_ID,1,3), From_Line_of_Business, To_Line_of_Business
           );
            lnInsertRec := SQL%ROWCOUNT;
            COMMIT;
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                DBMS_OUTPUT.put_line ( 'No records in raw data table' );
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for insert Xfer_Dashboard 3' );
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
      END;

      BEGIN
          insert into irpt.CR_Xfer_Dashboard
          ( SORT_ORDER,
            LOCATION,
            FROM_LINE_OF_BUSINESS,
            TO_LINE_OF_BUSINESS,
            ANSWERED,
            TRANSFERRED,
            Advisor_ID,
            Advisor_Name
          )
          ( select 2 Sort_Order, substr(Advisor_ID,1,3) Loc, null, null, sum(INBOUND_COUNT) Answered, 0, Advisor_ID, Advisor_Name
              from irpt.CR_ActualStaff_Result
             where start_date >= (alnfday_ofmonth)
               AND start_date <= (alnlday_ofmonth)
               and Agg_Level   = 'H'
               and substr( advisor_ID,1,3) in (select Site_Code from irpt.work_site)
               and Line_of_Business not like '%IVR%'
               and Line_of_Business not in ('XM','Default')
             group by substr(Advisor_ID,1,3), Advisor_ID, Advisor_Name
            having sum(INBOUND_COUNT) > 0
           );
            lnInsertRec := SQL%ROWCOUNT;
            COMMIT;
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                DBMS_OUTPUT.put_line ( 'No records in raw data table' );
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for insert Xfer_Dashboard 4' );
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
      END;

      FOR lmdashboard_2 IN c_dashboard_2
        LOOP
        BEGIN
          UPDATE irpt.CR_Xfer_Dashboard
             SET Transferred = lmdashboard_2.Transferred
           WHERE Advisor_ID            = lmdashboard_2.Advisor_ID;
          lnUpdated := SQL%ROWCOUNT;
          COMMIT;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              perform_update := 0;
            WHEN OTHERS THEN
              gnErrNum := SQLCODE;
              gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
              DBMS_OUTPUT.put_line ( 'TERMINATED: exception for insert Xfer_Dashboard 5');
              DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
              DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
              ROLLBACK;
              RETURN ( -1 );
        END;
        END LOOP;

      Delete
        from irpt.CR_Xfer_Dashboard
       where transferred = 0
         and advisor_ID is not null;

      BEGIN
          insert into irpt.CR_Xfer_Dashboard
          ( SORT_ORDER,
            LOCATION,
            FROM_LINE_OF_BUSINESS,
            TO_LINE_OF_BUSINESS,
            ANSWERED,
            TRANSFERRED,
            Advisor_ID,
            Advisor_Name
          )
          ( select 2 Sort_Order, substr(From_Advisor_ID,1,3) Loc, null, To_Line_of_Business, null, count (*) Transferred, From_Advisor_ID Advisor_ID, From_Advisor_Name Advisor_Name
              from irpt.infomart_xfer
             where run_date >= (alnfday_ofmonth)
               AND run_date <= (alnlday_ofmonth)
               and substr( from_advisor_ID,1,3) in (select Site_Code from irpt.work_site)
               and From_Line_of_Business not like '%IVR%'
               and From_Line_of_Business not in ('XM','Default','Case_Work')
               and To_Line_of_Business not like '%IVR%'
               and To_Line_of_Business not in ('XM','Default')
               and Flag_Conn = 1
               and Disposition in ('Consult', 'Consult + Transfer','Answered')
             group by substr(From_Advisor_ID,1,3), From_Advisor_ID, From_Advisor_Name, To_Line_of_Business
           );
            lnInsertRec := SQL%ROWCOUNT;
            COMMIT;
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                DBMS_OUTPUT.put_line ( 'No records in raw data table' );
            WHEN OTHERS
            THEN
                gnErrNum := SQLCODE;
                gvcErrMsg := SUBSTR ( SQLERRM, 1, 100 );
                DBMS_OUTPUT.put_line ( 'TERMINATED: exception for insert Xfer_Dashboard 6' );
                DBMS_OUTPUT.put_line ( 'ERROR NUM ' || gnErrNum );
                DBMS_OUTPUT.put_line ( 'ERROR MSG ' || gvcErrMsg );
                ROLLBACK;
                RETURN ( -1 );
      END;

      UPDATE irpt.CR_Xfer_Dashboard
         SET FROM_LINE_OF_BUSINESS = Advisor_ID ||' '|| Advisor_Name
       WHERE Advisor_ID is not null;
      COMMIT;

      RETURN lnInsertRec;
  END Xfer_Dashboard;

END IRPT_CR_ActualStaff;
/
