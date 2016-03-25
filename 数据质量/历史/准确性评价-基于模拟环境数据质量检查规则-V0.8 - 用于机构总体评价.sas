%include "E:\�½��ļ���\SAS\CONFIG.sas";
/*libname nfcs "D:\����\201602";*/
proc sort data = nfcs.sino_loan(drop = sloantype SAREACODE dgetdate sloancompactcode scurrency iclass5stat iinfoindicator sname scerttype scertno skeepcolumn ipersonid smsgfilename ilineno stoporgcode istate ipbcstate WHERE=(SUBSTR(sorgcode,1,1)='Q' AND sorgcode not in ('Q10152900H0000','Q10152900H0001') and
 &firstday. > datepart(dbillingdate) >= &firstday_two.)) out = sino_loan;
by iloanid dbillingdate;
run;
%AddLabel(sino_loan);
data sino_loan;
 set sino_loan;
 error_flag = 0;
 doubt_flag = 0;
label
	&label.
;
run;
/*Rule 1*/
/*1������ʱ���ϱ�"����Ӧ��������"�Ƿ�ȡ�����գ���һ�������������������������һ�λ����ͬһ�죩*/
/*����*/
data sino_loan;
 set sino_loan;
  if dbillingdate ^= drecentpaydate and iaccountstat not in (1,2) then doubt_flag = 1;
;
run;
/*Rule 2*/
/*---2���������ڣ��ʻ�״̬ȴΪ����*/
/*����*/
data sino_loan;
 set sino_loan;
	if icurtermspastdue > 0 and iaccountstat = 1 then error_flag = 1;
run;

/*Rule 3*/
/*5���ڷǿ������µ�����£����һ��ʵ�ʻ������ڴ��ڵ���Ӧ�������ڣ���ʵ�ʻ�����Ϊ0 */
/*��ȷ��һ�����һ��ʵ�ʻ�������ȡֵ���⣩(�����ã������ڵı���)*/
/*����*/
data sino_loan;
 set sino_loan;
	if dbillingdate <= drecentpaydate and iactualpayamount = 0 and drecentpaydate ^= ddateopened and sPaystat24month ^= '///////////////////////*' then doubt_flag = 1;
run;

/*Rule 4*/
/*---8������δ���ڣ�����Ӧ��������ڱ���ʵ�ʻ������"�˻�״̬"����*/
/*����*/
data sino_loan;
 set sino_loan;
if iaccountstat = 1 and ddateclosed ^= dbillingdate and ischeduledamount > iACTUALPAYAMOUNT then error_flag = 1;
run;

/*Rule 5*/
/*9��T+1����ʱ��"����Ӧ��������"��"���һ�λ�������"Ӧ�õ���"��������" ����T+1�����ã���T+1������--�����軹���û��������������ϱ�һ�Σ�*/
/*����*/
/*��Ҫ����*/
data sino_loan;
 set sino_loan;
if sPaystat24month = '///////////////////////*' and (dbillingdate ^= ddateopened or drecentpaydate ^= ddateopened) then error_flag = 1;
run; 

/*Rule 6*/
/*10��24�»���״̬���һλ��Nʱ����"����Ӧ������"����"ʵ�ʻ�����"*/
/*����*/
data sino_loan;
 set sino_loan;
if substr(sPaystat24month,24,1) = 'N' and ischeduledamount > iactualpayamount then doubt_flag = 1;
run; 

/*Rule 7*/
/*11���ǰ��»���������ºͽ������⣬"����Ӧ��������"��Ӧ��ȡÿ�����һ��*/
/*����*/
data sino_loan;
 set sino_loan;
 if substr(sPaystat24month,1,23) ^= '///////////////////////' and sTermsfreq ^= '03' and iaccountstat ^= 3 and datepart(dbillingdate) ^= intnx('month',datepart(dbillingdate),0,'end') 
then doubt_flag = 1;
run;
 
/*Rule 8*/
/*14��"ʵ�ʻ�����"����""����Ӧ������""����δ�������⽻�׶�*/
/*����*/
proc sql;
	update sino_loan
 set doubt_flag = 1
where iActualpayamount > ischeduledamount and dbillingdate < ddateclosed and intnx('month',datepart(dbillingdate),0,'b') ^= intnx('month',datepart(ddateclosed),0,'b')
   and substr(sPaystat24month,23,1) in ('*','#','/','N') and iloanid not in (select iloanid from nfcs.sino_loan_spec_trade where speculiartradetype in ('4','5','9'))
;
quit;

/*Rule 9*/
/*15���������ʱ��ʵ��Ӧ�����ӦΪ0*/
/*����*/
data sino_loan;
 set sino_loan;
if substr(sPaystat24month,24,1) = 'C' and iaccountstat = 3 and iactualpayamount = 0 then error_flag = 1;
run;

/*Rule 10*/
/*16����"����Ӧ������"Ϊ0����"����ʵ�ʻ�����"����0������£�24�»���״̬��ӦΪ�Ǻ�*/
/*����*/
data sino_loan;
 set sino_loan;
if ischeduledamount = 0 and iactualpayamount > 0 and substr(sPaystat24month,24, 1) = '*' then error_flag = 1;
run;

/*Rule 11*/
/*18���������������������δ����ʱ����ǰ�����ܶ�Ӧ�õ���""����Ӧ������""��"ʵ�ʻ�����"֮��*/
/*����*/
data sino_loan;
 set sino_loan;
if substr(sPaystat24month, 23, 2) = 'N1' and (ischeduledamount - iactualpayamount > iamountpastdue + 1 or ischeduledamount - iactualpayamount < iamountpastdue - 1) 
then error_flag = 1;
run;

/*Rule 12*/
/*19�����»����������,"����Ӧ������"��Ӧ��Ϊ0(�����������)*/
/*����*/
data sino_loan;
 set sino_loan;
 if sPaystat24month ^= '///////////////////////*' and sTermsfreq = '03' and ischeduledamount=0  then error_flag = 1;
run;

/*Rule 13*/
/*20�����»���ں�"����Ӧ��������"�������µ�*/
/*����*/
data sino_loan;
 set sino_loan;
 if sTermsfreq = '03' and datepart(dbillingdate) ^= intnx('month',datepart(dbillingdate),0,'end') and iaccountstat ^= 3 
	and intnx('month',datepart(dbillingdate),0,'end') > intnx('month',datepart(ddateclosed),0,'end') then error_flag = 1;
run;

/*Rule 14*/
/*22�����ں�"�ۼ���������"��"��ǰ��������"��"�����������"��Ӧ�ü����ۼ�*/
/*����*/
/*��Ҫ����*/
proc sql;
create table rule_14 as select
iloanid
,iid
,ddateclosed    ,
dbillingdate    ,
drecentpaydate   ,  
iamountpastdue   ,
icurtermspastdue ,
itermspastdue ,
imaxtermspastdue ,
iaccountstat  ,
sPaystat24month
  from nfcs.sino_loan(where = (iaccountstat = 2 and intnx('month',datepart(ddateclosed),0,'end') <= intnx('month',datepart(dbillingdate),0,'end')))
   order by iloanid,dbillingdate
;
quit;
data rule_14;
	set rule_14;
	if iloanid = lag(iloanid) and dbillingdate > lag(dbillingdate) and icurtermspastdue <= lag(icurtermspastdue) and itermspastdue <= lag(itermspastdue) and imaxtermspastdue <= lag(imaxtermspastdue) then delete;
run;
proc sql;
	create table rule_14_t as select
		iloanid
		,count(iloanid) as cnt
		from rule_14
		group by iloanid
		having calculated cnt > 1
	;
quit;
proc sql;
	create table rule_14 as select
		*
		from rule_14 as a
		where iloanid in (select iloanid from rule_14_t)
   order by iloanid,dbillingdate
	;
quit;
proc sql;
	update sino_loan
	set error_flag = 1
	where iid in (select iid from rule_14)
;
quit;

/*Rule 15*/
/*23��24�»���״̬���Ϊ1ʱ��31-60δ�黹����Ӧ��Ϊ0*/
/*����*/
data sino_loan;
 set sino_loan;
 if substr(sPaystat24month,24,1) = '1' and iamountpastdue30 ^= 0 then error_flag = 1;
run;

 /*Rule 16*/
/*26������ʱ,"����Ӧ��������"�����������һ��ʵ�ʻ�������*/
/*����*/
data sino_loan;
 set sino_loan;
 if  drecentpaydate ^= dbillingdate and iaccountstat = 3 then doubt_flag = 1;
run;

 /*Rule 17*/
/*27��"ʵ�ʻ�����"���ڵ���"����Ӧ������"ʱ��24���»���״̬ȡֵ��׼ȷ*/
/*����*/
data sino_loan;
 set sino_loan;
 if  iactualpayamount >= ischeduledamount
   and substr(sPaystat24month, 23, 1) in ('*','#','/','N')
   and substr(sPaystat24month, 24, 1) not in ('*','#','/','N','C')
then error_flag = 1;
run;

 /*Rule 18*/
/*29�����»����˻����ϸ������ڱ����˻�����������£�"ʵ�ʻ�����"Ӧ�ô���"����Ӧ������"*/
/*����*/
data sino_loan;
 set sino_loan;
 if  substr(sPaystat24month,24,1)='N' and substr(sPaystat24month,23,1) not in ('*','#','/','N') and iactualpayamount<=ischeduledamount and dbillingdate <= ddateclosed
then error_flag = 1;
run;

/*Rule 19*/
/*33�����»����֮��,��ǰ�����ܶӦ��С�����*/
/*����*/
data sino_loan;
 set sino_loan;
 if sTermsfreq = '03' and dbillingdate > ddateclosed and (iamountpastdue < ibalance)
then error_flag = 1;
run;

/*Rule 20*/
/*34��"����Ƶ��"Ϊ�̶���"��ǰ��������"��"�ۼ���������"��Ӧ�ô��ڻ�������*/
/*����*/
data sino_loan;
 set sino_loan;
 if sTermsfreq not in ('07', '08', '99') and input(smonthduration,4.) < itermspastdue
then doubt_flag = 1;
run;

/*Rule 21*/
/*39���ǿ����£����һ��"ʵ�ʻ�������"��Ӧ������"����Ӧ��������"*/
/*����*/
data sino_loan;
 set sino_loan;
 if dbillingdate>ddateopened and drecentpaydate>dbillingdate
then doubt_flag = 1;
run;

/*Rule 22*/
/*40����������Ĵ��"����Ӧ������"Ӧ�õ���"ʵ�ʻ�����"*/
/*����*/
data sino_loan;
 set sino_loan;
 if substr(sPaystat24month,23,1) in ('*','#','/','N') and iaccountstat = 3 and ischeduledamount ^= iactualpayamount  and  ddateclosed = dbillingdate
then error_flag = 1;
run;

/*����У�����*/

/*Rule 23*/
/*��ͬ�����ʹ��ͬһ����ҵ��ŵ�����*/
/*����*/
PROC SORT DATA=nfcs.SINO_LOAN(KEEP=iloanid ddateopened scertno)  OUT=rule_23_t;
BY iloanid scertno;
RUN;
data rule_23_t;
	set rule_23_t;
	if iloanid = lag(iloanid) and ddateopened = lag(ddateopened) and scertno ^= lag(scertno);
run;
proc sql;
	update sino_loan
	set doubt_flag = 1
	where iloanid in (select distinct iloanid from rule_23_t)
;
quit;


/*Rule 24*/
/*ͬһ����ҵ��Ĳ�ͬ����ʹ�ò�ͬҵ��ŵ�����*/
/*����*/
PROC SORT DATA=nfcs.SINO_LOAN(KEEP= iid sorgcode scertno SACCOUNT ddateopened dbillingdate icreditlimit ibalance)  OUT=rule_24_t;
BY SORGCODE scertno SACCOUNT;
RUN;
data rule_24_t;
	set rule_24_t;
	if sorgcode= lag(sorgcode) and scertno = lag(scertno) and SACCOUNT ^= lag(saccount) and ICREDITLIMIT = lag(ICREDITLIMIT) and ddateopened = lag(ddateopened);
run;
proc sql;
	update sino_loan
	set doubt_flag = 1
	where iid in (select iid from rule_24_t)
;
quit;

/*Rule 25*/
/*��ʱ��*/
/*δ���Ĵ���ҵ���嵥*/
/*����*/
proc sort data = nfcs.sino_loan(keep = sorgcode iloanid dbillingdate IACCOUNTSTAT where = (SUBSTR(sorgcode,1,1)='Q' AND sorgcode not in ('Q10152900H0000','Q10152900H0001'))) out= loan_all;
by iloanid descending dbillingdate;
run;
data loan_all;
	set loan_all;
	if iloanid = lag(iloanid) then delete;
	if IACCOUNTSTAT not in (1,2) then delete;
run;
proc sql;
	create table loan_in as select
		T1.iloanid
		,T1.sorgcode
		,(case when T2.iloanid is null then 0 else 1 end) as jishixing_label
	from loan_all as T1
	left join sino_loan as T2
	on T1.iloanid = T2.iloanid
;
quit;

proc sql;
	create table _loan_m_sta_ as select
		t2.shortname label = "�������"
		,t2.person
		,T1.sorgcode label = "��������"
		,count(t1.iloanid) as total label = "Ӧ���ҵ������"
		,sum(t1.jishixing_label) as in_nfcs label = "�����ҵ������"
		,round(calculated in_nfcs/calculated total,0.0001) as in_per label = "��ʱ��" format = percent8.2 informat = percent8.2
	from loan_in as t1
	left join config as T2
	on T1.sorgcode = T2.sorgcode
	group by shortname
	order by total desc
;
quit;
data _loan_m_sta_;
	set _loan_m_sta_;
	if sorgcode = lag(sorgcode) then delete;
run;
/*���*/
proc sql;
	create table zqx_org as select
	T2.shortname label = "�������"
	,T2.person
/*	,sum(1,- sum(doubt_flag)/count(*)) as doubt_per label = "������׼ȷ��-����" format = percent8.2 informat = percent8.2 */
	,count(T1.sorgcode) as record_cnt label = "����ҵ���¼����"
	,sum(t1.error_flag) as record_cnt_error label = "��������������¼����"
	,1 - calculated record_cnt_error/calculated record_cnt as error_per label = "׼ȷ��" format = percent8.2 informat = percent8.2
	,T2.total
	,t2.in_nfcs
	,T2.in_per
	from sino_loan as T1
	left join _loan_m_sta_ as T2
	on T1.sorgcode = T2.sorgcode
	group by T1.sorgcode
;
quit;
data zqx_org;
	set zqx_org;
	if shortname = lag(shortname) then delete;
run;
proc sort data = zqx_org;
by desending record_cnt;
run;

ods listing off;
 ods tagsets.excelxp file = "&outfile.�����߼�У�������_&currmonth..xls" style = printer
      options(sheet_name="�����߼�У�������" embedded_titles='yes' embedded_footnotes='yes' sheet_interval="bygroup" frozen_headers='yes' frozen_rowheaders='1' autofit_height='yes');
proc report data = zqx_org NOWINDOWS headline headskip
          style(header)={background=lightgray foreground=black font_weight=bold};
title "�����߼�У�������";
	columns _all_;
	define person /display 'ר��Ա';
	define shortname/ display width=5;
	define record_cnt_error/ display '�������������/��¼����';
	define in_per/display center;
	define error_per/display center;
	compute after;
 	if in_per >= 0.9 and error_per >= 0.99 then 
 		call define(_row_,'style','style={background=lightyellow fontweight=bold}');
 	endcomp;
footnote '��׼ȷ�ʡ�����������Ϊ�����󡿵Ĺ���';
run;
ods tagsets.excelxp close;
  ods listing;


