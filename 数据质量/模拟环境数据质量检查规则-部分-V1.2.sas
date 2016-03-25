%include "E:/�ּ���/code/config.sas";
%let lib = %str(work);
/*%let orgfilter = %nrstr(and sorgcode in ('Q10151000H3000' 'Q10152900H0900'));*/
%let orgfilter = %nrstr(and sorgcode in ('Q10152900H9800' 'Q10151000H8800' 'Q10152900H1D00' 'Q10152900HT400' 'Q10153300HDW00' 'Q10152900HFJ00' 'Q10152900H2Z00' 'Q10152900H1W00' 'Q10152900H8500' 'Q10152900H0900' 'Q10151000H3000' 'Q10152900HN500' 'Q10152900HAZ00' 'Q10152900H1200' 'Q10152900H1W00' 'Q10153900H7T00' 'Q10152900HC000' 'Q10151000H0G00' 'Q10155800HZ200' 'Q10152900H9C00' 'Q10155800H2P00' 'Q10152900HAL00' 'Q10152900HN300' 'Q10155800H5400' 'Q10152900H3500' 'Q10155800HCV00'
'Q10155800HS000' 'Q10152900H1400' 'Q10151000H0Y00' 'Q10152900HD900' 'Q10155800H3200' 'Q10152900H0900' 'Q10152900HU700' 'Q10151000H2800' 'Q10152900H7C00' 'Q10155800H6800' 'Q10151000HV200'));
data _null_;
	if %sysfunc(length(&orgfilter.)) = 0 then orgfilter = " ";
run;
%let timefilter = %str(and dgetdate >= &firstday_three. and &firstday. > datepart(dbillingdate) >= &firstday_three.);
%let NoteAddr = %unquote(%str(E:\�ּ���\code\��������\�����������ϵͳ������˵��-V1.7.docx));

/*���������ļ���*/
/*д�ɺ�*/

/*�����ݴ�NFCS���г�ȡ�����ݲֿ���*/
proc sort data = nfcs.sino_loan(drop = scurrency iclass5stat iinfoindicator skeepcolumn ipersonid ilineno stoporgcode ipbcstate WHERE=(SUBSTR(sorgcode,1,1)='Q' 
and ISTATE = 0 AND sorgcode not in ('Q10152900H0000','Q10152900H0001') &orgfilter. )) out = &lib..sino_loan;
by iloanid dbillingdate descending dgetdate;
run;
data &lib..sino_loan;
informat zhangqi yymmn6.;
format zhangqi yymmn6.;
 set &lib..sino_loan;
 zhangqi = intnx('month',datepart(dbillingdate),0,'b');
;
run;
proc sort data = &lib..sino_loan;
	by iloanid zhangqi descending dgetdate;
run;
data &lib..sino_loan;
	set &lib..sino_loan;
	if iloanid = lag(iloanid) and zhangqi = lag(zhangqi) then delete;
run;
/*Rule 1*/
/*1������ʱ���ϱ�"����Ӧ��������"�Ƿ�ȡ�����գ���һ�������������������������һ�λ����ͬһ�죩*/
/*����*/
proc sql;
create table rule_1 as
select t.sorgcode  label = "��������",
		smsgfilename label = "��������",
       t.saccount  label = "ҵ���", 
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       t.sTermsfreq     label = "����Ƶ��",
       t.dbillingdate   label = "����Ӧ��������",
       t.drecentpaydate label = "���һ��ʵ�ʻ�������",
       t.ischeduledamount label = "����Ӧ������",
       t.iactualpayamount label = "����ʵ�ʻ�����",
       t.iaccountstat   label = "�˻�״̬",
       t.sPaystat24month label = "��ʮ���»���״̬"
  from &lib..sino_loan(where = (dbillingdate ^= drecentpaydate and iaccountstat not in (1,2) and dgetdate >= mdy(7,1,2013) &orgfilter.)) as t
;
quit;

/*Rule 2*/
/*2���������ڣ��ʻ�״̬ȴΪ����*/
/*����*/
proc sql;
create table rule_2 as 
select a.sorgcode     label = "��������",
		smsgfilename label = "��������",
       a.saccount  label = "ҵ���", 
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.dbillingdate     label = "����Ӧ��������",
/*       a.drecentpaydate   label = "���һ�λ�������",*/
/*       a.ischeduledamount label = "����Ӧ������",*/
/*       a.iactualpayamount label = "����ʵ�ʻ�����",*/
/*       a.ibalance        label =  "�������",*/
       a.icurtermspastdue label = "��ǰ��������",
/*       a.itermspastdue    label = "�ۼ���������",*/
/*       a.imaxtermspastdue label = "�����������",*/
       a.iaccountstat     label = "�˻�״̬",
       a.sPaystat24month  label = "��ʮ���»���״̬"
  from  &lib..sino_loan(where = (icurtermspastdue > 0 and iaccountstat = 1 &timefilter. &orgfilter.)) as a
  order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 3*/
/*5���ڷǿ������µ�����£����һ��ʵ�ʻ������ڴ��ڵ��ڽ���Ӧ�������ڣ���ʵ�ʻ�����Ϊ0 */
/*��ȷ��һ�����һ��ʵ�ʻ�������ȡֵ���⣩(�����ã������ڵı���)*/
/*����*/
proc sql;
create table rule_3 as
select a.sorgcode label = "��������",
		smsgfilename label = "��������",
a.saccount label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ��ʵ�ʻ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = "����ʵ�ʻ�����", 
a.iaccountstat  label = "�˻�״̬",
a.sPaystat24month label = "��ʮ���»���״̬"
from &lib..sino_loan(where = (dbillingdate <= drecentpaydate and iactualpayamount = 0 and drecentpaydate ^= ddateopened and sPaystat24month ^= '///////////////////////*' &timefilter. &orgfilter.)) as a
  order by sorgcode,saccount,dbillingdate
;
 quit;

/*Rule 4*/
/*8������δ���ڣ�����Ӧ��������ڱ���ʵ�ʻ������"�˻�״̬"����*/
/*����*/
proc sql;
create table rule_4 as select
		t.sorgcode label = "��������",
		smsgfilename label = "��������",
		t.saccount         label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       t.dbillingdate     label = "����Ӧ��������",
       t.drecentpaydate   label = "���һ��ʵ�ʻ�������",
       t.ischeduledamount label = "����Ӧ������",
       t.iactualpayamount  label = "����ʵ�ʻ�����", 
       t.iaccountstat     label = "�˻�״̬",
       t.sPaystat24month  label = "��ʮ���»���״̬"
  from &lib..sino_loan (where = (iaccountstat = 1 and ddateclosed ^= dbillingdate and ischeduledamount > iACTUALPAYAMOUNT &timefilter. &orgfilter.)) as t
    order by sorgcode,saccount,dbillingdate

;
quit;

/*Rule 5*/
/*9��T+1����ʱ��"����Ӧ��������"��"���һ�λ�������"Ӧ�õ���"��������" ����T+1�����ã���T+1������--�����軹���û��������������ϱ�һ�Σ�*/
/*����*/
/*��Ҫ����*/
proc sql;
create table rule_5 as select
a.sorgcode label = "��������",
smsgfilename label = "��������",
a.saccount label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������" format = DATETIME20. informat = DATETIME20.,
a.drecentpaydate label = '���һ�λ�������' format = DATETIME20. informat = DATETIME20.,
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = "����ʵ�ʻ�����",
a.sPaystat24month label = '��ʮ���»���״̬'
  from &lib..sino_loan (where = (sPaystat24month = '///////////////////////*' and (dbillingdate ^= ddateopened or drecentpaydate ^= ddateopened) &timefilter. &orgfilter.)) as a
     order by sorgcode,saccount,dbillingdate
;
quit; 

/*Rule 6*/
/*10��24�»���״̬���һλ��Nʱ����"����Ӧ������"����"����ʵ�ʻ�����"*/
/*����*/
proc sql;
create table rule_6 as select
 a.sorgcode label = "��������",
 		smsgfilename label = "��������",
a.saccount label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = 'ʵ�ʻ�����',
a.sPaystat24month label = '��ʮ���»���״̬'
  from &lib..sino_loan(where = (substr(sPaystat24month,24,1) = 'N' and ischeduledamount > iactualpayamount &timefilter. &orgfilter.)) as a
 order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 7*/
/*11���ǰ��»���������ºͽ������⣬"����Ӧ��������"��Ӧ��ȡÿ�����һ��*/
/*����*/
proc sql;
create table rule_7 as select
a.sorgcode label = "��������",
smsgfilename label = "��������",
a.saccount label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = 'ʵ�ʻ�����',
a.iaccountstat   label = "�˻�״̬",
a.sPaystat24month label = '��ʮ���»���״̬'
  from &lib..sino_loan(where = (substr(sPaystat24month,1,23) ^= '///////////////////////' and sTermsfreq ^= '03' and iaccountstat ^= 3 and datepart(dbillingdate) ^= intnx('month',datepart(dbillingdate),0,'end') &timefilter. &orgfilter.)) as a
 order by sorgcode,saccount,dbillingdate
;
quit;

 
/*Rule 8*/
/*14��"����ʵ�ʻ�����"����""����Ӧ������""����δ�������⽻�׶�*/
/*����*/
proc sql;
create table rule_8 as select
		a.sorgcode     label = "��������",
				smsgfilename label = "��������",
       a.saccount         label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq       label = "����Ƶ��",
       a.dbillingdate     label = "����Ӧ��������",
       a.drecentpaydate   label = "���һ�λ�������",
 	   a.ischeduledamount label = "����Ӧ������",
       a.iactualpayamount label = "����ʵ�ʻ�����",
       a.sPaystat24month  label = "��ʮ���»���״̬"
  from &lib..sino_loan(where = (1=1 &timefilter. &orgfilter.)) as a
where iActualpayamount > ischeduledamount and dbillingdate < ddateclosed and intnx('month',datepart(dbillingdate),0,'b') ^= intnx('month',datepart(ddateclosed),0,'b')
   and substr(sPaystat24month,23,1) in ('*','#','/','N') and iloanid not in (select iloanid from nfcs.sino_loan_spec_trade where speculiartradetype in ('4','5','9'))
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 9*/
/*15���������ʱ��ʵ��Ӧ�����ӦΪ0*/
/*����*/
proc sql;
create table rule_9 as select
		a.sorgcode     label = "��������",
				smsgfilename label = "��������",
       a.saccount         label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq       label = "����Ƶ��",
       a.dbillingdate     label = "����Ӧ��������",
       a.drecentpaydate   label = "���һ�λ�������",
       a.ischeduledamount label = "����Ӧ������",
       a.iactualpayamount label = "����ʵ�ʻ�����",
       a.sPaystat24month  label = '��ʮ���»���״̬'
  from &lib..sino_loan(where = (substr(sPaystat24month,24,1) = 'C' and iaccountstat = 3 and iactualpayamount = 0 &timefilter. &orgfilter.)) as a
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 10*/
/*16����"����Ӧ������"Ϊ0����"����ʵ�ʻ�����"����0������£�24�»���״̬��ӦΪ�Ǻ�*/
/*����*/
proc sql;
create table rule_10 as select
a.sorgcode label = "��������",
		smsgfilename label = "��������",
a.saccount label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = 'ʵ�ʻ�����',
a.sPaystat24month label = '��ʮ���»���״̬'
  from &lib..sino_loan(where = (ischeduledamount = 0 and iactualpayamount > 0 and substr(sPaystat24month,24, 1) = '*' &timefilter. &orgfilter.)) as a
/* where a.ischeduledamount = 0 and a.iactualpayamount > 0 and substr(a.sPaystat24month,24, 1) = '*' &timefilter. &orgfilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 11*/
/*18���������������������δ����ʱ����ǰ�����ܶ�Ӧ�õ���"����Ӧ������"��"����ʵ�ʻ�����"֮��*/
/*����*/
proc sql;
create table rule_11 as select
a.sorgcode     label = "��������",   
		smsgfilename label = "��������",
a.saccount         label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq       label = "����Ƶ��",
a.dbillingdate     label = "����Ӧ��������",
a.drecentpaydate   label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������", 
a.iactualpayamount label = "����ʵ�ʻ�����",
a.iamountpastdue   label = '��ǰ�����ܶ�',
a.sPaystat24month  label = '��ʮ���»���״̬'  
from &lib..sino_loan(where = (substr(sPaystat24month, 23, 2) = 'N1'
 and (ischeduledamount - iactualpayamount > iamountpastdue + 1 or ischeduledamount - iactualpayamount < iamountpastdue - 1) &timefilter. &orgfilter.)) as a
/* where substr(a.sPaystat24month, 23, 2) = 'N1'*/
/* and (ischeduledamount - iactualpayamount ^= iamountpastdue + 1 and ischeduledamount - iactualpayamount ^= iamountpastdue - 1 and ischeduledamount - iactualpayamount ^= iamountpastdue) &timefilter. &orgfilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 12*/
/*19�����»����������,"����Ӧ������"��Ӧ��Ϊ0(�����������)���ѽ�����������ǰ���������ų���*/
/*����*/
proc sql;
create table rule_12 as select
a.sorgcode label = "��������",
		smsgfilename label = "��������",
a.saccount label = "ҵ���",
scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
datepart(a.dbillingdate) as dbillingdate label = "����Ӧ��������"  format = yymmdd10. informat = yymmdd10.,
a.drecentpaydate label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = 'ʵ�ʻ�����',
a.sPaystat24month label = '��ʮ���»���״̬'
  from &lib..sino_loan(where = (sPaystat24month ^= '///////////////////////*' and sTermsfreq = '03' and ischeduledamount=0 and intnx('month',datepart(dbillingdate),0,'end') ^= intnx('month',datepart(ddateopened),0,'end')
&timefilter. &orgfilter.)) as a
/* where a.sPaystat24month ^= '///////////////////////*' and a.sTermsfreq = '03' and a.ischeduledamount=0 &timefilter. &orgfilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 13*/
/*20�����»���ں�"����Ӧ��������"�������µ�*/
/*����*/
proc sql;
create table rule_13 as select
		a.sorgcode label = "��������",
				smsgfilename label = "��������",
       a.saccount label = "ҵ���",
	   	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq label = "����Ƶ��",
       a.dbillingdate label = "����Ӧ��������",
       a.drecentpaydate label = "���һ�λ�������",
       a.ischeduledamount label = "����Ӧ������",       
		a.iactualpayamount label = "����ʵ�ʻ�����",
       a.sPaystat24month label = '��ʮ���»���״̬'
  from &lib..sino_loan(where = (sTermsfreq = '03' and datepart(dbillingdate) ^= intnx('month',datepart(dbillingdate),0,'end') and iaccountstat ^= 3 
	and intnx('month',datepart(dbillingdate),0,'end') > intnx('month',datepart(ddateclosed),0,'end') &timefilter. &orgfilter.)) as a
/* where a.sTermsfreq = '03' and datepart(a.dbillingdate) ^= intnx('month',datepart(a.dbillingdate),0,'end') and a.iaccountstat ^= 3 */
/*	and intnx('month',datepart(a.dbillingdate),0,'end') > intnx('month',datepart(a.ddateclosed),0,'end') &timefilter. &orgfilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 14*/
/*22�����ں�"�ۼ���������"��"��ǰ��������"��"�����������"��Ӧ�ü����ۼ�*/
/*����*/
/*ɸѡ���ں�δ�����ҵ�񣬲���ҵ��š���������*/
proc sql;
create table rule_14 as select
a.sorgcode     label = "��������",
smsgfilename label = "��������",
a.saccount         label = "ҵ���",
scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq       label = "����Ƶ��",
a.dbillingdate     label = "����Ӧ��������",
a.drecentpaydate   label = "���һ�λ�������",  
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = "����ʵ�ʻ�����",
a.iamountpastdue   label = '��ǰ�����ܶ�',
a.icurtermspastdue label = "��ǰ��������",
a.itermspastdue label = "�ۼ���������",
a.imaxtermspastdue label = "�����������",
a.iaccountstat   label = "�˻�״̬",
a.sPaystat24month  label = '��ʮ���»���״̬',
a.iloanid
  from &lib..sino_loan(where = (iaccountstat = 2 and intnx('month',datepart(ddateclosed),0,'end') <= intnx('month',datepart(dbillingdate),0,'end') &timefilter. &orgfilter.)) as a
   order by sorgcode,saccount,dbillingdate
;
quit;
/*�������������統ǰ���ڡ��ۼ����ڡ������������û�м����ۼӵģ���ɾ��*/
data rule_14;
	set rule_14;
	if sorgcode = lag(sorgcode) and saccount = lag(saccount) and dbillingdate > lag(dbillingdate) and icurtermspastdue <= lag(icurtermspastdue) 
and itermspastdue <= lag(itermspastdue) and imaxtermspastdue <= lag(imaxtermspastdue) then delete;
run;
/*ͳ��ʣ���ҵ�������Ϲ�ҵ��Ӧ��ֻʣ�������ڵ��µ����ڣ����Ϲ�ҵ�������������1����¼���Ϲ�ҵ���iloanid*/
proc sql;
	create table rule_14_t as select
		iloanid
		,count(iloanid) as cnt
		from rule_14
		group by iloanid
		having calculated cnt > 1
	;
quit;
/*�����Ϲ�ҵ���ں������ȫ��ɸѡ��*/
proc sql;
	create table rule_14 as select
		a.*
		from rule_14 as a
		where a.iloanid in (select iloanid from rule_14_t)
	;
quit;
/*V1.0���-�絽��ҵ����δ���ͣ���δ������ǰ����Ĵ���ҵ���ڵ��ڵ��º��͵ģ������޷��б��ں��һ�����ڵ�ǰ���ڡ��ۼ����ڡ�������������Ƿ���ȷ��
��������ԭ�򣬽���ҵ���ں��һ��ҵ����ϻ��ɱ�ǩ �����*/
proc sort data = rule_14;
	by iloanid dbillingdate;
run;
data rule_14;
	set rule_14(drop = iloanid);
	if first.dbillingdate then delete;
run;

/*Rule 15*/
/*23��24�»���״̬���Ϊ1ʱ��31-60δ�黹����Ӧ��Ϊ0*/
/*����*/
proc sql;
create table rule_15 as select
a.sorgcode label = "��������",
		smsgfilename label = "��������",
a.saccount label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.iamountpastdue30 label = '����31��60��δ�黹����',
a.sPaystat24month label = '��ʮ���»���״̬'
  from &lib..sino_loan( where  = (substr(sPaystat24month,24,1) = '1' and iamountpastdue30 ^= 0 &timefilter. &orgfilter.)) as a
/* where substr(a.sPaystat24month,24,1) = '1' and a.iamountpastdue30 ^= 0 &timefilter. &orgfilter.*/
  order by sorgcode,saccount,dbillingdate
;
quit;

 /*Rule 16*/
/*26������ʱ,"����Ӧ��������"�����������һ��ʵ�ʻ�������*/
/*����*/
proc sql;
create table rule_16 as select
a.sorgcode label = "��������",
		smsgfilename label = "��������",
a.saccount label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = 'ʵ�ʻ�����',
a.sPaystat24month label = '��ʮ���»���״̬'
  from &lib..sino_loan(where = (drecentpaydate ^= dbillingdate and iaccountstat = 3 &timefilter. &orgfilter.)) as a
   order by sorgcode,saccount,dbillingdate
;
quit;

 /*Rule 17*/
/*27��"����ʵ�ʻ�����"���ڵ���"����Ӧ������"ʱ��24���»���״̬ȡֵ��׼ȷ*/
/*����*/
proc sql;
create table rule_17 as select
	   a.sorgcode     label = "��������",
	   		smsgfilename label = "��������",
       a.saccount         label = "ҵ���",
	   	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq       label = "����Ƶ��",
       a.dbillingdate     label = "����Ӧ��������",
       a.drecentpaydate   label = "���һ�λ�������",
       a.ischeduledamount label = '����Ӧ������',
       a.iactualpayamount label = "����ʵ�ʻ�����",
       a.sPaystat24month  label = '��ʮ���»���״̬'
from &lib..sino_loan( where = (iactualpayamount >= ischeduledamount
   and substr(sPaystat24month, 23, 1) in ('*','#','/','N')
   and substr(sPaystat24month, 24, 1) not in ('*','#','/','N','C') &timefilter. &orgfilter.)) as a
/* where  a.iactualpayamount >= a.ischeduledamount*/
/*   and substr(a.sPaystat24month, 23, 1) in ('*','#','/','N')*/
/*   and substr(a.sPaystat24month, 24, 1) not in ('*','#','/','N','C') &timefilter. &orgfilter.*/
    order by sorgcode,saccount,dbillingdate

;
quit;

 /*Rule 18*/
/*29�����»����˻����ϸ������ڱ����˻�����������£�"����ʵ�ʻ�����"Ӧ�ô���"����Ӧ������"*/
/*����*/
proc sql;
create table rule_18 as select
a.sorgcode label = "��������",
		smsgfilename label = "��������",
a.saccount label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label ="����ʵ�ʻ�����", 
a.sPaystat24month label = "��ʮ���»���״̬"
  from &lib..sino_loan(where = (substr(sPaystat24month,24,1)='N' and substr(sPaystat24month,23,1) not in ('*','#','/','N') and iactualpayamount<=ischeduledamount and dbillingdate <= ddateclosed &timefilter. &orgfilter.)) as a 
/*where substr(a.sPaystat24month,24,1)='N' and substr(a.sPaystat24month,23,1) not in ('*','#','/','N') and a.iactualpayamount<=a.ischeduledamount and a.dbillingdate <= a.ddateclosed &timefilter. &orgfilter.*/
 order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 19*/
/*33�����»����֮��,��ǰ�����ܶӦ��С�����*/
/*����*/
proc sql;
create table rule_19 as select
a.sorgcode label = "��������",
		smsgfilename label = "��������",
a.saccount label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.ibalance label = '���',
a.iamountpastdue label = '��ǰ�����ܶ�',
a.iamountpastdue30 label = '����31_60��δ�黹����',
a.iamountpastdue60 label = '����61_90��δ�黹����',
a.iamountpastdue90 label = '����91_80��δ�黹����',
a.iamountpastdue180  label = '����180��δ�黹����',
a.sPaystat24month label = '��ʮ���»���״̬'
  from &lib..sino_loan(where = ( sTermsfreq = '03' and dbillingdate > ddateclosed and (iamountpastdue < ibalance) &timefilter. &orgfilter.)) as a
/* where a.sTermsfreq = '03' and a.dbillingdate > a.ddateclosed and (a.iamountpastdue < a.ibalance) &timefilter. &orgfilter.*/
  order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 20*/
/*34��"����Ƶ��"Ϊ�̶���"��ǰ��������"��"�ۼ���������"��Ӧ�ô��ڻ�������*/
/*����*/
proc sql;
create table rule_20 as select
sorgcode     label = "��������",  
		smsgfilename label = "��������",
saccount  label = "ҵ���", 
scertno label = '֤������',
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
sTermsfreq   label = "����Ƶ��",
dbillingdate     label = "����Ӧ��������",  
drecentpaydate   label = "���һ��ʵ�ʻ�������",    
smonthduration  label = '��������',  
icurtermspastdue label = "��ǰ��������",
itermspastdue    label = "�ۼ���������",  
imaxtermspastdue label = "�����������"
from &lib..sino_loan(where = (sTermsfreq not in ('07', '08', '99') and input(smonthduration,4.) < itermspastdue &timefilter. &orgfilter.))
/*where sTermsfreq not in ('07', '08', '99') and input(smonthduration,4.) < itermspastdue &timefilter. &orgfilter.*/
order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 21*/
/*37��������ʽΪ����Ȼ�˱�֤��δ�ϱ�������*/
/*����*/
proc sql;
create table rule_21 as select
c.sorgcode  label = "��������",
		smsgfilename label = "��������",
c.saccount      label = "ҵ���",
  scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
c.iguaranteeway label = "������ʽ"
  from &lib..sino_loan(where = (1=1 &timefilter. &orgfilter.)) as c 
where c.iguaranteeway in (3, 5, 7) and c.iloanid not in (select iloanid from nfcs.sino_loan_guarantee)
order by sorgcode,saccount
;
quit;

/*Rule 22*/
/*39���ǿ����£����һ��"ʵ�ʻ�������"��Ӧ������"����Ӧ��������"*/
/*����*/
proc sql;
create table rule_22 as select
a.sorgcode label = "��������",
		smsgfilename label = "��������",
a.saccount label = "ҵ���",
	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.sPaystat24month label = '��ʮ���»���״̬'
from &lib..sino_loan(where = (dbillingdate>ddateopened and drecentpaydate>dbillingdate &timefilter. &orgfilter.)) as a
/*where a.dbillingdate>a.ddateopened and a.drecentpaydate>a.dbillingdate &timefilter. &orgfilter.*/
;
quit;

/*Rule 23*/
/*40����������Ĵ��"����Ӧ������"Ӧ�õ���"����ʵ�ʻ�����"*/
/*����*/
proc sql;
create table rule_23 as select
 	   a.sorgcode label = "��������",
	   		smsgfilename label = "��������",
       a.saccount label = "ҵ���",
	   	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq label = "����Ƶ��",
       a.dbillingdate label = "����Ӧ��������",
       a.ischeduledamount label = "����Ӧ������",
       a.iactualpayamount label = "����ʵ�ʻ�����",
	   a.iaccountstat   label = "�˻�״̬",
       a.sPaystat24month "��ʮ�ĸ��»���״̬"
  from &lib..sino_loan( where = (substr(sPaystat24month,23,1) in ('*','#','/','N') and iaccountstat = 3 and  ischeduledamount ^= iactualpayamount  and  ddateclosed = dbillingdate &timefilter. &orgfilter.)) as a
/* where substr(a.sPaystat24month,23,1) in ('*','#','/','N') and  a.ischeduledamount ^= a.iactualpayamount  and  a.ddateclosed = a.dbillingdate &timefilter. &orgfilter.*/
 order by sorgcode,saccount,dbillingdate
 ;
 quit;

 /*Rule 24*/
/*48�������ص�Ӧ�õ����м�*/
/* ����*/
 proc sql;
create table rule_24 as select
 t.sorgcode label = "��������",
 		smsgfilename label = "��������",
 t.saccount label = "ҵ���",
 datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
 dbillingdate label = "����Ӧ��������",
 t.sareacode label = '�����ص�'
  from &lib..sino_loan(where = (substr(sareacode, 3, 4) = '0000' &timefilter. &orgfilter.)) as t
/* where substr(sareacode, 3, 4) = '0000' &timefilter. &orgfilter.*/
 order by sorgcode,saccount
;
quit;

/*41���������Ź��������涨�޿ͻ�����ͬ�������������벻�ܲɼ���������Ĭ��Ϊ�ռ���*/
/*NFCSû�ж�Ӧ�涨*/

/*����У�����*/

/*Rule 25*/
/*��ʱ��*/
/*δ���Ĵ���ҵ���嵥*/
/*����*/
proc sql;
	create table dmonth as select
	distinct(intnx('month',datepart(DBILLINGDATE),0,'b')) as dmonth FORMAT=yymmn6. INFORMAT=yymmn6.
	from &lib..sino_loan(keep = sorgcode dgetdate DBILLINGDATE where = (1=1 &timefilter. &orgfilter.))
/*	where today() > calculated dmonth > mdy(7,1,2013)*/
;
quit;
/*��Ҫά�� ��proc expand����ѿ�����*/
proc sql;
	create table sino_loan_1 as select
		substr(sorgcode,1,14) as sorgcode
		,saccount
		,scertno
		,intnx('month',datepart(DDATEOPENED),0,'b') as omonth FORMAT=yymmn6. INFORMAT=yymmn6.
		,intnx('month',datepart(DDATECLOSED),0,'b') as cmonth FORMAT=yymmn6. INFORMAT=yymmn6.
		,intnx('month',datepart(DBILLINGDATE),0,'b') as dmonth FORMAT=yymmn6. INFORMAT=yymmn6.
	from &lib..sino_loan(keep = scertno SORGCODE saccount dgetdate DDATEOPENED DDATECLOSED DBILLINGDATE iaccountstat where=(sorgcode like 'Q%' and datepart(DBILLINGDATE) < today() and iaccountstat in (1,2) &timefilter. &orgfilter.))
	order by saccount,dmonth
;
quit;
data sino_loan_1;
	set sino_loan_1;
	if saccount = lag(saccount) and dmonth = lag(dmonth) then delete;
run;
proc sql;
	create table sorgcodesaccount_ as select
		sorgcode
		,saccount
		,scertno
		,intnx('month',datepart(DDATEOPENED),0,'b') as omonth FORMAT=yymmn6. INFORMAT=yymmn6.
		,intnx('month',datepart(DDATECLOSED),0,'b') as cmonth FORMAT=yymmn6. INFORMAT=yymmn6.
	from &lib..sino_loan(keep = scertno SORGCODE saccount dgetdate DDATEOPENED DDATECLOSED DBILLINGDATE iaccountstat where=(sorgcode like 'Q%' and datepart(DBILLINGDATE) < today() and iaccountstat in (1,2)))
;
quit;

proc sort in=sorgcodesaccount_ nodupkey;
by sorgcode saccount;
run;

proc sql;
	create table sino_loan_2 as select 
		Sorgcodesaccount_.SORGCODE
		,Sorgcodesaccount_.saccount
		,sorgcodesaccount_.scertno
		,Sorgcodesaccount_.omonth
		,Sorgcodesaccount_.cmonth
		,dmonth.dmonth
		from Sorgcodesaccount_,dmonth;
quit;

data sino_loan_2;
	length sorgcode_1 $14.;
	set sino_loan_2;
		if omonth<=dmonth and dmonth<=cmonth;
	sorgcode_1=substr(sorgcode,1,14);
	drop sorgcode;
	rename sorgcode_1 = sorgcode;
run;
proc sort in=sino_loan_2;
	by sorgcode saccount dmonth;
run;
proc sort in=sino_loan_1 nodupkey;
	by sorgcode saccount dmonth;
run;
proc sql;
	create table rule_25 as select
		t1.sorgcode label='��������'
		,t1.saccount label='ҵ���'
		,t1.scertno label = '֤������'
		,t1.omonth label='����ҵ�����·�'
		,t1.cmonth label='����ҵ����ֹ�·�'
		,t1.dmonth label='δ�������'
/*		,t2.dmonth as dmonth1*/
/*		,(case when t2.dmonth is not null then 1 else 0 end) as status label='���״̬'*/
		from sino_loan_2 as t1
		left join sino_loan_1 as t2
		on t1.sorgcode=t2.sorgcode and t1.saccount=t2.saccount and t1.dmonth=t2.dmonth
		where t2.dmonth is null
;
quit;

/*Rule 26*/
/*��ͬ�����ʹ��ͬһ����ҵ��ŵ�����*/
/*����*/
PROC SORT DATA=&lib..SINO_LOAN(KEEP= smsgfilename iloanid sorgcode SACCOUNT dgetdate ddateopened dbillingdate icreditlimit ibalance sname scerttype scertno)  OUT=rule_26_t;
BY SORGCODE SACCOUNT scertno;
RUN;

data rule_26_t2;
	set rule_26_t(WHERE=(SUBSTR(SORGCODE,1,1)='Q' AND SORGCODE^='Q10152900H0000' AND SORGCODE^='Q10152900H0001'));
	if sorgcode = lag(sorgcode) and SACCOUNT = lag(SACCOUNT) and ddateopened = lag(ddateopened) and scertno ^= lag(scertno);
	label
	sorgcode = ��������
	smsgfilename = ��������
	SACCOUNT = ҵ���
	sname = ����
	scerttype = ֤������
	scertno = ֤������
	ddateopened = ҵ��������
	dbillingdate = ����/Ӧ��������
	icreditlimit = ���Ŷ��
	ibalance = ���
	;
run;
proc sql;
	create table rule_26 as select
	*
	from rule_26_t
	where iloanid in (select iloanid from rule_26_t2) &timefilter. 
;
quit;
data rule_26;
retain sorgcode SACCOUNT;
	set rule_26(drop = iloanid);
label
	sorgcode = ��������
		smsgfilename = ��������
	dgetdate = ��������
	SACCOUNT = ҵ���
	sname = ����
	scerttype = ֤������
	scertno = ֤������
	ddateopened = ҵ��������
	dbillingdate = ����/Ӧ��������
	icreditlimit = ���Ŷ��
	ibalance = ���
	;
run;

/*Rule 27*/
/*ͬһ����ҵ��Ĳ�ͬ����ʹ�ò�ͬҵ��ŵ�����*/
/*����*/
PROC SORT DATA=&lib..SINO_LOAN(KEEP= sorgcode smsgfilename sname scerttype scertno SACCOUNT ddateopened dgetdate dbillingdate icreditlimit ibalance)  OUT=rule_27_t;
BY SORGCODE scertno SACCOUNT;
RUN;
data rule_27_t2;
	set rule_27_t(where = (1=1 &timefilter. &orgfilter.));
	if sorgcode= lag(sorgcode) and scertno = lag(scertno) and SACCOUNT ^= lag(saccount) and ICREDITLIMIT = lag(ICREDITLIMIT) and ddateopened = lag(ddateopened);
	label
	sorgcode = ��������
	smsgfilename = ��������
	SACCOUNT = ҵ���
	sname = ����
	scerttype = ֤������
	scertno = ֤������
	ddateopened = ҵ��������
	dbillingdate = ����/Ӧ��������
	icreditlimit = ���Ŷ��
	ibalance = ���
	;
run;
/**/
proc sql;
	create table Rule_27 as select
	*
	from rule_27_t
	where cats("-",sorgcode,scertno) in (select distinct cats("-",sorgcode,scertno) from rule_27_t2)
;
quit;
data Rule_27;
retain sorgcode SACCOUNT;
	set Rule_27(where = (1=1 &timefilter. &orgfilter.));
	label
	sorgcode = ��������
	smsgfilename = ��������
	dgetdate = ��������
	SACCOUNT = ҵ���
	sname = ����
	scerttype = ֤������
	scertno = ֤������
	ddateopened = ҵ��������
	dbillingdate = ����/Ӧ��������
	icreditlimit = ���Ŷ��
	ibalance = ���
	;
run;


/*Rule 28*/
/*ÿ�ڻ�����*����/���Ŷ�������ʱ���߼���ϵ�������⣨�껯��������Ӧ�ô���6%-60%�ĺ���Χ�ڣ�*/
/*����*/
proc sort data = &lib..sino_loan(keep = sorgcode saccount dgetdate dbillingdate SMONTHDURATION icreditlimit ITREATYPAYAMOUNT STREATYPAYDUE WHERE=(SUBSTR(sorgcode,1,1)='Q' AND sorgcode not in ('Q10152900H0000','Q10152900H0001'))) out = rule_28_t nodupkey;
by sorgcode saccount dbillingdate;
run;
data rule_28_t;
	set rule_28_t(where = (1=1 &timefilter. &orgfilter.));
	if sorgcode =lag(sorgcode) and saccount = lag(saccount) then delete;
run;

data rule_28;
	format interest percent8.2;
	informat interest percent8.2;
	format interest_year_single percent8.2;
	informat interest_year_single percent8.2;
	format STREATYPAYDUE_num 2.;
	format ITREATYPAYAMOUNT_num best12.;
set rule_28_t;
	if STREATYPAYDUE in ('U' 'X') or ITREATYPAYAMOUNT = 'U' then delete;
	if STREATYPAYDUE = 'O' then STREATYPAYDUE_num = 1;
	STREATYPAYDUE_num = input(STREATYPAYDUE,2.);
	ITREATYPAYAMOUNT_NUM = INPUT(ITREATYPAYAMOUNT,BEST12.);
	interest = round((ITREATYPAYAMOUNT * STREATYPAYDUE / ICREDITLIMIT - 1),0.0001);
	MONTHDURATION = input(SMONTHDURATION,4.);
/*	if interest <= 0 then delete;*/
	interest_year_single = round(interest * 12 /MONTHDURATION,0.0001);
	if 0.06 <= interest_year_single <=0.6 then delete;
	label
	dgetdate = ��������
	saccount = ҵ���
	interest_year_single = �껯������(�ٷֱ�)
	STREATYPAYDUE_num = Э����������_����
	ITREATYPAYAMOUNT_num = Э���ڻ����_����
	ICREDITLIMIT = ���Ŷ��
	SMONTHDURATION = ��������
	STREATYPAYDUE = Э����������
	ITREATYPAYAMOUNT = Э���ڻ����
	dbillingdate = ����/Ӧ��������
	;
run;

data rule_28;
retain sorgcode saccount;
	set rule_28(drop = interest_year_single interest STREATYPAYDUE_num ITREATYPAYAMOUNT_num MONTHDURATION);
run;

/*Rule 29*/
/*����ҵ��ġ�����/Ӧ�������ڡ���Ӧ���ڱ����ϴ�ʱ��*/
/*����*/
proc sql;
    create table rule_29_temp as select
    T1.sorgcode label = "��������"
    ,T1.saccount label = "ҵ���",
	datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
	datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
	datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.
    ,T1.dbillingdate  label = "����Ӧ��������"
    ,T2.duploadtime label = "�����ϴ�ʱ��"
    from &lib..sino_loan(where = (1=1 &timefilter. &orgfilter.)) as T1
    left join nfcs.sino_msg as T2
    on T1.SMSGFILENAME = T2.SMSGFILENAME and T1.dbillingdate > T2.duploadtime and T2.duploadtime is not null
;
quit;

proc sql;
	create table rule_29 as select
	T1.sorgcode label = "��������"
	,T1.saccount label = "ҵ���",
	datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.
	,T1.dbillingdate  label = "����Ӧ��������"
	,T2.duploadtime label = "�����ϴ�ʱ��"
	from &lib..sino_loan as T1
	left join nfcs.sino_msg as T2
	on T1.SMSGFILENAME = T2.SMSGFILENAME 
	where T2.duploadtime is not null and datepart(T1.dbillingdate) > datepart(duploadtime)
	order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 30*/
/*���������Ӧ����һ������ķ�Χ�ڣ�����1990�꣬���ڱ����ϴ�ʱ��*/
/*����*/
proc sql;
	create table rule_30 as select
	T1.sorgcode label = "��������"
	,T1.saccount label = "ҵ���",
	datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
	datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
	datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.
	,T1.dbillingdate  label = "����Ӧ��������"
	,T2.duploadtime label = "���ļ���ʱ��"
	from &lib..sino_loan(where = (1=1 &timefilter. &orgfilter.)) as T1
	left join nfcs.sino_msg as T2
	on T1.SMSGFILENAME = T2.SMSGFILENAME 
	where T2.duploadtime is not null and datepart(T1.ddateopened) > datepart(duploadtime) or datepart(T1.ddateopened) < mdy(1,1,1990)
	order by sorgcode,saccount,dbillingdate
;
quit; 

/*Rule 31*/
/*���˻�����Ϣ�еģ����������ڡ�Ӧ���ں���ķ�Χ�ڣ�����1935�꣬����2005��*/
/*����*/
proc sql;
	create table rule_31 as select
	T1.sorgcode label = "��������"
	,T1.sname label = '����'
	,T1.scerttype label = '֤������'
	,T1.scertno label = '֤������'
	,T1.spin
	,T2.dbirthday label = '��������'
	from nfcs.sino_person_certification as T1
	left join nfcs.sino_person as T2
	on T1.spin = T2.spin and T1.sorgcode = T2.sorgcode
	where T2.dbirthday <= mdy(1,1,1935) or T2.dbirthday > mdy(1,1,2005)
;
quit;
data rule_31;
	set rule_31;
	if spin = lag(spin) then delete;
drop
spin
;
run;

/*Rule 32*/
/*���˻�����Ϣ�еģ����������ڡ�Ӧ�ú����֤�������������Ϣ����һ��*/
/*����*/
PROC SQL;
	CREATE TABLE rule_32 AS SELECT
	T1.sorgcode label = '��������'
	,T1.sname label = '����'
	,T1.scerttype label = '֤������'
	,T1.scertno label = '֤������'
	,T2.dbirthday label = "��������"
	FROM nfcs.sino_person_certification(where = (SUBSTR(SORGCODE,1,1)='Q' AND SORGCODE not in ('Q10152900H0000' 'Q10152900H0001'))) AS T1
	left JOIN nfcs.sino_person AS T2
	ON T1.spin = T2.spin and T1.sorgcode = T2.sorgcode
	where length(SCERTNO)=18 and MDY(input(SUBSTR(SCERTNO,11,2),2.),input(SUBSTR(SCERTNO,13,2),2.),input(SUBSTR(SCERTNO,7,4),4.)) ^= DATEPART(dbirthday) and scerttype ='0' and DBIRTHDAY is not null
	order by scertno
;
QUIT;
data rule_32;
	set rule_32;
	if scertno = lag(scertno) then delete;
run;

*Rule 33
/*����*/
У��24�»���״̬����ȷ��
;

data rule_33_t;
	set &lib..sino_loan(keep = iid SPAYSTAT24MONTH );
	SPAYSTAT_flag = 0;
	array SPAYSTAT{*} $1. X1-X24;
	do i =1 to 24;
	SPAYSTAT{i} = substr(SPAYSTAT24MONTH,i,1);
	end;
/*24�»���״̬������λ�����*/
	do j =2 to 24;
	if 1 <= input(SPAYSTAT{j-1},1.) <=7 and input(SPAYSTAT{j},1.) not in ('C' 'G') 
	and input(SPAYSTAT{j},1.) - input(SPAYSTAT{j-1},1.) > 1 then SPAYSTAT_flag = 1;
	else if SPAYSTAT{j-1} in ('N' '*' '/') and input(SPAYSTAT{j},1.) > 2 then SPAYSTAT_flag = 1;
	end;
drop
i
j
X1-X24
;
run; 

proc sql;
	create table rule_33 as select
 	   a.sorgcode label = "��������",
	  	smsgfilename label = "��������",
       a.saccount label = "ҵ���",
	   	   scertno label = '֤������' format = $18., 
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq label = "����Ƶ��",
       a.dbillingdate label = "����Ӧ��������",
       a.ischeduledamount label = "����Ӧ������",
       a.iactualpayamount label = "����ʵ�ʻ�����",
	   a.iaccountstat   label = "�˻�״̬",
       a.sPaystat24month "��ʮ�ĸ��»���״̬"
	   from &lib..sino_loan(where = (1=1 &timefilter.)) as A
	   left join rule_33_t as B
	   on A.iid = B.iid
	   where B.SPAYSTAT_flag = 1
	;
quit;
	
/*������*/

proc sql noprint;
	select count(distinct sorgcode) into :socnumber
	from config(where = (1=1 &orgfilter.))
	where person ^= ''
	;
quit;
proc sql noprint;
	select count(distinct person) into :personnumber
	from config(where = (1=1 &orgfilter.))
	where person ^= ''
	;
quit;

data _null_;
set config(where = (person ^= '' &orgfilter.));
	suffix=put(_n_,5.);
	retain index 0;
	if person = lag(person) then index = index;
	else index = index + 1;
	call symput(cats('sorgcode',suffix),%sysfunc(trim(sorgcode)));
	call symput(cats('shortname',suffix),shortname);
/*	call symput(cats('attachment',suffix),attachment);*/
	call symput(cats('person',suffix),%sysfunc(trim(person)));
/*	call symput(cats('person',index),person);*/
/*	call symput(cats('mailaddress',index),mailaddress);*/
run;
%put &shortname1.;
%put &person1.;

/*������������ļ���*/
%macro shortnamefile;
%do i = 1 %to &socnumber.;
%ChkFile("&outfile.%sysfunc(strip(&&shortname&i.))");
%end;
%mend;

/*ɸѡʱ���*/
%macro timefilter;
%do i = 1 %to 33;
	data rule_&i.;
		set rule_&i.(where = (dgetdate >= mdy(7,1,2013) and &firstday. > datepart(dbillingdate) >= &firstday_two.) );
/*		if  &firstday. > datepart(dbillingdate) >= &firstday_two.;*/
	run;
%end;
%mend;

/*������������������ݵĶ�Ӧ��ϵ��*/

/*���*/
%macro outfile;
%do i = 1 %to 33;
	%do j = 1 %to &socnumber.;
data work.&&sorgcode&j.;
	set rule_&i.(where = (sorgcode = "%sysfunc(strip(&&sorgcode&j.))"));
drop 
sorgcode
;
run;
%let dsid = %sysfunc(open(%sysfunc(trim(&&sorgcode&j.))));
    %if &dsid %then %do;
            %let nobs=%sysfunc(attrn(&dsid,nobs));
            %let rc=%sysfunc(close(&dsid));
			%if &nobs. = 0 %then %do;
				proc delete data=work.&&sorgcode&j.;
				run;
			%end;
			%else %do;
/*data _null_;*/
/*x 'xcopy "D:\�߼�У����\testify_template.xlsx &outfile.%sysfunc(strip(&&shortname&j.))\rule_&i..xlsx /C /Y" & exit';*/
/*run;*/
libname xls excel "&outfile.%sysfunc(strip(&&shortname&j.))\rule_&i..xlsx";
data xls.sheet1(dblabel=yes);
	set work.%sysfunc(trim(&&sorgcode&j.));
;
run;
libname xls clear;
proc delete data=work.&&sorgcode&j.;
run;
			%end;
		%end;
/*	%exit:*/
	%end;
%end;
%mend;
%macro copynote;
%do i = 1 %to &socnumber.;
x "xcopy &NoteAddr. &outfile.%sysfunc(strip(&&shortname&i.)) /C /Y";
%end;
%mend;

%put &NoteAddr.;

/*��������ƴ��*/
/*Ҫע��WINRAR�Ƿ�ռ��*/
%macro zipfile;
data _null_;  
/*    rc =system("cd &file_path."); */
%do i = 1 %to &socnumber.;
	rc = system("'C:\Program Files\WinRAR\winrar.exe' a -ad -icbk -ep -y &outfile.%sysfunc(strip(&&shortname&i.))_&currmonth..rar &outfile.%sysfunc(strip(&&shortname&i.))\");
%end;
/*%do i = 1 %to &socnumber.;*/
/*	rc =system("'C:/Program Files/WinRAR/winrar.exe' a -ad -icbk -ep -y &outfile.strip(&&shortname&i.)_&currmonth..rar &NoteAddr.");*/
/**/
/*%end;*/
/*stop;*/
%do j = 1 %to &socnumber.;
/*	rc =system("'C:/Program Files/WinRAR/winrar.exe' a -ad -icbk -ep -ieq &outfile.&&&person&&j.._&currmonth..rar &outfile.%sysfunc(strip(&&shortname&j.))_&currmonth..rar");*/
	rc =system("'C:\Program Files\WinRAR\winrar.exe' a -ad -icbk -ep -y -df &outfile.%sysfunc(strip(&&person&j.))_&currmonth..rar &outfile.%sysfunc(strip(&&shortname&j.))_&currmonth..rar");

%end;
/*return;*/
run;  
%mend;  

/*data _null_;*/
/*x "@echo off*/
/*net use \\137.168.99.116\ipc$ 1qaz2WSX /user.administrator";*/
/*x 'xcopy  ';*/
/*run;*/


/*����*/
ods listing close;
ods results off;
ODS TRACE OFF;
proc printto log=_null_;
run;
%shortnamefile;
/*%timefilter;*/
%outfile;
%copynote;
%zipfile;
proc printto log= log;
run;
ods listing;
ods results on;
ODS TRACE ON;

