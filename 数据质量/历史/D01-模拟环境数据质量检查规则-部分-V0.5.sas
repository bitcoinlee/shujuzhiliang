%include "E:/�½��ļ���/SAS/config.sas";


/*��ȡ�����ļ�*/

/*proc sql noprint;*/
/*	select count(distinct sorgcode) into :orgnumber*/
/*	from nfcs.sino_msg*/
/*	;*/
/*quit;*/
/*data _null_;*/
/*set config;*/
/*	suffix=put(_n_,5.);*/
/*	retain index 1;*/
/*	if manager = lag(manager) then index = index;*/
/*	else index = index + 1;*/
/*	call symput(cats('rarfilename',suffix),rarfilename);*/
/*	call symput(cats('report',suffix),report);*/
/*	call symput(cats('attachment',suffix),attachment);*/
/*	call symput(cats('manager',index),manager);*/
/*	call symput(cats('mailaddress',index),mailaddress);*/
/*	call symput(cats('org',index),org);*/
/*run;*/

/**/


/*���������ļ���*/
/*д�ɺ�*/


/*�����ݴ�NFCS���г�ȡ�����ݲֿ���*/
/*��ʱ�������������*/
/*proc sort data = nfcs.sino_loan(drop = iid dgetdate sloancompactcode scurrency iclass5stat iinfoindicator sname scerttype scertno skeepcolumn ipersonid smsgfilename ilineno stoporgcode istate ipbcstate WHERE=(SUBSTR(sorgcode,1,1)='Q' AND sorgcode not in ('Q10152900H0000','Q10152900H0001'))) out = sino_loan;*/
/*by iloanid dbillingdate;*/
/*run;*/
/*data loan_int_base;*/
/*	set loan_int_base;*/
/*	if sorgcode =lag(sorgcode) and saccount = lag(saccount) then delete;*/
/*run;*/


/*Rule 1*/
/*1������ʱ���ϱ�"����Ӧ��������"�Ƿ�ȡ�����գ���һ�������������������������һ�λ����ͬһ�죩*/
/*����*/
proc sql;
create table rule_1 as
select t.sorgcode  label = "��������",
       t.saccount  label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       t.sTermsfreq     label = "����Ƶ��",
/*       t.ddateopened    label = "��������",*/
/*       t.ddateclosed    label = "��������",*/
       t.dbillingdate   label = "����Ӧ��������",
       t.drecentpaydate label = "���һ��ʵ�ʻ�������",
       t.ischeduledamount label = "����Ӧ������",
       t.iactualpayamount label = "����ʵ�ʻ�����",
       t.iaccountstat   label = "�˻�״̬",
       t.sPaystat24month label = "��ʮ�ĸ��»���״̬"
  from nfcs.sino_loan(where = (dbillingdate ^= drecentpaydate and iaccountstat not in (1,2) &timefilter.)) as t
  order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 2*/
/*---2���������ڣ��ʻ�״̬ȴΪ����*/
/*����*/
proc sql;
create table rule_2 as 
select a.sorgcode     label = "��������",
       a.saccount         label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
/*       a.ddateopened      label = "��������",*/
/*       a.ddateclosed      label = "��������",*/
/*       a.sTermsfreq       label = "����Ƶ��",*/
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
  from  nfcs.sino_loan(where = (icurtermspastdue > 0 and iaccountstat = 1 &timefilter.)) as a
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
a.saccount label = "ҵ���",
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
from nfcs.sino_loan(where = (dbillingdate <= drecentpaydate and iactualpayamount = 0 and drecentpaydate ^= ddateopened and sPaystat24month ^= '///////////////////////*' &timefilter.)) as a
  order by sorgcode,saccount,dbillingdate
;
 quit;

/*Rule 4*/
/*---8������δ���ڣ�����Ӧ��������ڱ���ʵ�ʻ������"�˻�״̬"����*/
/*����*/
proc sql;
create table rule_4 as select
		t.sorgcode label = "��������",
		t.saccount         label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       t.dbillingdate     label = "����Ӧ��������",
       t.drecentpaydate   label = "���һ��ʵ�ʻ�������",
       t.ischeduledamount label = "����Ӧ������",
       t.iactualpayamount  label = "����ʵ�ʻ�����", 
       t.iaccountstat     label = "�˻�״̬",
       t.sPaystat24month  label = "��ʮ�ĸ��»���״̬"
  from nfcs.sino_loan (where = (iaccountstat = 1 and ddateclosed ^= dbillingdate and ischeduledamount > iACTUALPAYAMOUNT &timefilter.)) as t
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
a.saccount label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������" format = DATETIME20. informat = DATETIME20.,
a.drecentpaydate label = '���һ�λ�������' format = DATETIME20. informat = DATETIME20.,
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = "����ʵ�ʻ�����",
a.sPaystat24month label = '��ʮ���»���״̬'
  from nfcs.sino_loan (where = (sPaystat24month = '///////////////////////*' and (dbillingdate ^= ddateopened or drecentpaydate ^= ddateopened) &timefilter.)) as a
     order by sorgcode,saccount,dbillingdate
;
quit; 

/*Rule 6*/
/*10��24�»���״̬���һλ��Nʱ����"����Ӧ������"����"����ʵ�ʻ�����"*/
/*����*/
proc sql;
create table rule_6 as select
 a.sorgcode label = "��������",
a.saccount label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = 'ʵ�ʻ�����',
a.sPaystat24month label = '��ʮ���»���״̬'
  from nfcs.sino_loan(where = (substr(sPaystat24month,24,1) = 'N' and ischeduledamount > iactualpayamount &timefilter.)) as a
 order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 7*/
/*11���ǰ��»���������ºͽ������⣬"����Ӧ��������"��Ӧ��ȡÿ�����һ��*/
/*����*/
proc sql;
create table rule_7 as select
a.sorgcode label = "��������",
a.saccount label = "ҵ���",
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
  from nfcs.sino_loan(where = (substr(sPaystat24month,1,23) ^= '///////////////////////' and sTermsfreq ^= '03' and iaccountstat ^= 3 and datepart(dbillingdate) ^= intnx('month',datepart(dbillingdate),0,'end') &timefilter.)) as a
 order by sorgcode,saccount,dbillingdate
;
quit;

 
/*Rule 8*/
/*14��"����ʵ�ʻ�����"����""����Ӧ������""����δ�������⽻�׶�*/
/*����*/
proc sql;
create table rule_8 as select
		a.sorgcode     label = "��������",
       a.saccount         label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq       label = "����Ƶ��",
       a.dbillingdate     label = "����Ӧ��������",
       a.drecentpaydate   label = "���һ�λ�������",
 	   a.ischeduledamount label = "����Ӧ������",
       a.iactualpayamount label = "����ʵ�ʻ�����",
       a.sPaystat24month  label = "��ʮ���»���״̬"
  from nfcs.sino_loan(where = (1=1 &timefilter.)) as a
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
       a.saccount         label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq       label = "����Ƶ��",
       a.dbillingdate     label = "����Ӧ��������",
       a.drecentpaydate   label = "���һ�λ�������",
       a.ischeduledamount label = "����Ӧ������",
       a.iactualpayamount label = "����ʵ�ʻ�����",
       a.sPaystat24month  label = '��ʮ���»���״̬'
  from nfcs.sino_loan(where = (substr(sPaystat24month,24,1) = 'C' and iaccountstat = 3 and iactualpayamount = 0 &timefilter.)) as a
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 10*/
/*16����"����Ӧ������"Ϊ0����"����ʵ�ʻ�����"����0������£�24�»���״̬��ӦΪ�Ǻ�*/
/*����*/
proc sql;
create table rule_10 as select
a.sorgcode label = "��������",
a.saccount label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = 'ʵ�ʻ�����',
a.sPaystat24month label = '��ʮ���»���״̬'
  from nfcs.sino_loan(where = (ischeduledamount = 0 and iactualpayamount > 0 and substr(sPaystat24month,24, 1) = '*' &timefilter.)) as a
/* where a.ischeduledamount = 0 and a.iactualpayamount > 0 and substr(a.sPaystat24month,24, 1) = '*' &timefilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 11*/
/*18���������������������δ����ʱ����ǰ�����ܶ�Ӧ�õ���""����Ӧ������""��"����ʵ�ʻ�����"֮��*/
/*����*/
proc sql;
create table rule_11 as select
a.sorgcode     label = "��������",   
a.saccount         label = "ҵ���",
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
from nfcs.sino_loan(where = (substr(sPaystat24month, 23, 2) = 'N1'
 and (ischeduledamount - iactualpayamount ^= iamountpastdue + 1 and ischeduledamount - iactualpayamount ^= iamountpastdue - 1 and ischeduledamount - iactualpayamount ^= iamountpastdue) &timefilter.)) as a
/* where substr(a.sPaystat24month, 23, 2) = 'N1'*/
/* and (ischeduledamount - iactualpayamount ^= iamountpastdue + 1 and ischeduledamount - iactualpayamount ^= iamountpastdue - 1 and ischeduledamount - iactualpayamount ^= iamountpastdue) &timefilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 12*/
/*19�����»����������,"����Ӧ������"��Ӧ��Ϊ0(�����������)*/
/*����*/
proc sql;
create table rule_12 as select
a.sorgcode label = "��������",
a.saccount label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = 'ʵ�ʻ�����',
a.sPaystat24month label = '��ʮ���»���״̬'
  from nfcs.sino_loan(where = (sPaystat24month ^= '///////////////////////*' and sTermsfreq = '03' and ischeduledamount=0 &timefilter.)) as a
/* where a.sPaystat24month ^= '///////////////////////*' and a.sTermsfreq = '03' and a.ischeduledamount=0 &timefilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 13*/
/*20�����»���ں�"����Ӧ��������"�������µ�*/
/*����*/
proc sql;
create table rule_13 as select
		a.sorgcode label = "��������",
       a.saccount label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq label = "����Ƶ��",
       a.dbillingdate label = "����Ӧ��������",
       a.drecentpaydate label = "���һ�λ�������",
       a.ischeduledamount label = "����Ӧ������",       
		a.iactualpayamount label = "����ʵ�ʻ�����",
       a.sPaystat24month label = '��ʮ���»���״̬'
  from nfcs.sino_loan(where = (sTermsfreq = '03' and datepart(dbillingdate) ^= intnx('month',datepart(dbillingdate),0,'end') and iaccountstat ^= 3 
	and intnx('month',datepart(dbillingdate),0,'end') > intnx('month',datepart(ddateclosed),0,'end') &timefilter.)) as a
/* where a.sTermsfreq = '03' and datepart(a.dbillingdate) ^= intnx('month',datepart(a.dbillingdate),0,'end') and a.iaccountstat ^= 3 */
/*	and intnx('month',datepart(a.dbillingdate),0,'end') > intnx('month',datepart(a.ddateclosed),0,'end') &timefilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 14*/
/*22�����ں�"�ۼ���������"��"��ǰ��������"��"�����������"��Ӧ�ü����ۼ�*/
/*����*/
/*��Ҫ����*/
proc sql;
create table rule_14 as select
 a.sorgcode     label = "��������",
a.saccount         label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
/*datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,*/
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
  from nfcs.sino_loan(where = (iaccountstat = 2 and intnx('month',datepart(ddateclosed),0,'end') <= intnx('month',datepart(dbillingdate),0,'end') &timefilter.)) as a
   order by sorgcode,saccount,dbillingdate
;
quit;
data rule_14;
	set rule_14;
	if sorgcode = lag(sorgcode) and saccount = lag(saccount) and dbillingdate > lag(dbillingdate) and icurtermspastdue <= lag(icurtermspastdue) and itermspastdue <= lag(itermspastdue) and imaxtermspastdue <= lag(imaxtermspastdue) then delete;
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
		a.*
		from rule_14 as a
		where a.iloanid in (select iloanid from rule_14_t)
	;
quit;
data rule_14;
	set rule_14(drop = iloanid);
run;

/*Rule 15*/
/*23��24�»���״̬���Ϊ1ʱ��31-60δ�黹����Ӧ��Ϊ0*/
/*����*/
proc sql;
create table rule_15 as select
a.sorgcode label = "��������",
a.saccount label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq as "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.iamountpastdue30 label = '����31��60��δ�黹����',
a.sPaystat24month label = '��ʮ���»���״̬'
  from nfcs.sino_loan( where  = (substr(sPaystat24month,24,1) = '1' and iamountpastdue30 ^= 0 &timefilter.)) as a
/* where substr(a.sPaystat24month,24,1) = '1' and a.iamountpastdue30 ^= 0 &timefilter.*/
  order by sorgcode,saccount,dbillingdate
;
quit;

 /*Rule 16*/
/*26������ʱ,"����Ӧ��������"�����������һ��ʵ�ʻ�������*/
/*����*/
proc sql;
create table rule_16 as select
a.sorgcode label = "��������",
a.saccount label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label = 'ʵ�ʻ�����',
a.sPaystat24month label = '��ʮ���»���״̬'
  from nfcs.sino_loan(where = (drecentpaydate ^= dbillingdate and iaccountstat = 3 &timefilter.)) as a
   order by sorgcode,saccount,dbillingdate
;
quit;

 /*Rule 17*/
/*27��"����ʵ�ʻ�����"���ڵ���"����Ӧ������"ʱ��24���»���״̬ȡֵ��׼ȷ*/
/*����*/
proc sql;
create table rule_17 as select
	   a.sorgcode     label = "��������",
       a.saccount         label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq       label = "����Ƶ��",
       a.dbillingdate     label = "����Ӧ��������",
       a.drecentpaydate   label = "���һ�λ�������",
       a.ischeduledamount label = '����Ӧ������',
       a.iactualpayamount label = "����ʵ�ʻ�����",
       a.sPaystat24month  label = '��ʮ���»���״̬'
from nfcs.sino_loan( where = (iactualpayamount >= ischeduledamount
   and substr(sPaystat24month, 23, 1) in ('*','#','/','N')
   and substr(sPaystat24month, 24, 1) not in ('*','#','/','N','C') &timefilter.)) as a
/* where  a.iactualpayamount >= a.ischeduledamount*/
/*   and substr(a.sPaystat24month, 23, 1) in ('*','#','/','N')*/
/*   and substr(a.sPaystat24month, 24, 1) not in ('*','#','/','N','C') &timefilter.*/
    order by sorgcode,saccount,dbillingdate

;
quit;

 /*Rule 18*/
/*29�����»����˻����ϸ������ڱ����˻�����������£�"����ʵ�ʻ�����"Ӧ�ô���"����Ӧ������"*/
/*����*/
proc sql;
create table rule_18 as select
a.sorgcode label = "��������",
a.saccount label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.ischeduledamount label = "����Ӧ������",
a.iactualpayamount label ="����ʵ�ʻ�����", 
a.sPaystat24month label = "��ʮ���»���״̬"
  from nfcs.sino_loan(where = (substr(sPaystat24month,24,1)='N' and substr(sPaystat24month,23,1) not in ('*','#','/','N') and iactualpayamount<=ischeduledamount and dbillingdate <= ddateclosed &timefilter.)) as a 
/*where substr(a.sPaystat24month,24,1)='N' and substr(a.sPaystat24month,23,1) not in ('*','#','/','N') and a.iactualpayamount<=a.ischeduledamount and a.dbillingdate <= a.ddateclosed &timefilter.*/
 order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 19*/
/*33�����»����֮��,��ǰ�����ܶӦ��С�����*/
/*����*/
proc sql;
create table rule_19 as select
a.sorgcode label = "��������",
a.saccount label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
/*datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,*/
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
  from nfcs.sino_loan(where = ( sTermsfreq = '03' and dbillingdate > ddateclosed and (iamountpastdue < ibalance) &timefilter.)) as a
/* where a.sTermsfreq = '03' and a.dbillingdate > a.ddateclosed and (a.iamountpastdue < a.ibalance) &timefilter.*/
  order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 20*/
/*34��"����Ƶ��"Ϊ�̶���"��ǰ��������"��"�ۼ���������"��Ӧ�ô��ڻ�������*/
/*����*/
proc sql;
create table rule_20 as select
sorgcode     label = "��������",  
saccount     label = "ҵ���",
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
from nfcs.sino_loan(where = (sTermsfreq not in ('07', '08', '99') and input(smonthduration,4.) < itermspastdue &timefilter.))
/*where sTermsfreq not in ('07', '08', '99') and input(smonthduration,4.) < itermspastdue &timefilter.*/
order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 21*/
/*37��������ʽΪ����Ȼ�˱�֤��δ�ϱ�������*/
/*����*/
proc sql;
create table rule_21 as select
c.sorgcode  label = "��������",
c.saccount      label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
c.iguaranteeway label = "������ʽ"
  from nfcs.sino_loan(where = (1=1 &timefilter.)) as c 
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
a.saccount label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "����Ƶ��",
a.dbillingdate label = "����Ӧ��������",
a.drecentpaydate label = "���һ�λ�������",
a.sPaystat24month label = '��ʮ���»���״̬'
from nfcs.sino_loan(where = (dbillingdate>ddateopened and drecentpaydate>dbillingdate &timefilter.)) as a
/*where a.dbillingdate>a.ddateopened and a.drecentpaydate>a.dbillingdate &timefilter.*/
;
quit;

/*Rule 23*/
/*40����������Ĵ��"����Ӧ������"Ӧ�õ���"����ʵ�ʻ�����"*/
/*����*/
proc sql;
create table rule_23 as select
 	   a.sorgcode label = "��������",
       a.saccount label = "ҵ���",
datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "��������" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "��������" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq label = "����Ƶ��",
       a.dbillingdate label = "����Ӧ��������",
       a.ischeduledamount label = "����Ӧ������",
       a.iactualpayamount label = "����ʵ�ʻ�����",
       a.sPaystat24month "��ʮ�ĸ��»���״̬"
  from nfcs.sino_loan( where = (substr(sPaystat24month,23,1) in ('*','#','/','N') and  ischeduledamount ^= iactualpayamount  and  ddateclosed = dbillingdate &timefilter.)) as a
/* where substr(a.sPaystat24month,23,1) in ('*','#','/','N') and  a.ischeduledamount ^= a.iactualpayamount  and  a.ddateclosed = a.dbillingdate &timefilter.*/
 order by sorgcode,saccount,dbillingdate
 ;
 quit;

 /*Rule 24*/
/*48�������ص�Ӧ�õ����м�*/
/* ����*/
 proc sql;
create table rule_24 as select
 t.sorgcode label = "��������",
 t.saccount label = "ҵ���",
 datepart(dgetdate) as dgetdate label = "��������" format = yymmdd10.,
 t.sareacode label = '�����ص�'
  from nfcs.sino_loan(where = (substr(sareacode, 3, 4) = '0000' &timefilter.)) as t
/* where substr(sareacode, 3, 4) = '0000' &timefilter.*/
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
	from nfcs.sino_loan(keep = dgetdate DBILLINGDATE where = (1=1 &timefilter.))
/*	where today() > calculated dmonth > mdy(7,1,2013)*/
;
quit;
/*��Ҫά�� ��proc expand����ѿ�����*/
proc sql;
	create table sino_loan_1 as select
		substr(sorgcode,1,14) as sorgcode
		,saccount
		,intnx('month',datepart(DDATEOPENED),0,'b') as omonth FORMAT=yymmn6. INFORMAT=yymmn6.
		,intnx('month',datepart(DDATECLOSED),0,'b') as cmonth FORMAT=yymmn6. INFORMAT=yymmn6.
		,intnx('month',datepart(DBILLINGDATE),0,'b') as dmonth FORMAT=yymmn6. INFORMAT=yymmn6.
	from nfcs.sino_loan(keep = SORGCODE saccount dgetdate DDATEOPENED DDATECLOSED DBILLINGDATE iaccountstat where=(sorgcode like 'Q%' and datepart(DBILLINGDATE) < today() and iaccountstat in (1,2) &timefilter.))
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
		,intnx('month',datepart(DDATEOPENED),0,'b') as omonth FORMAT=yymmn6. INFORMAT=yymmn6.
		,intnx('month',datepart(DDATECLOSED),0,'b') as cmonth FORMAT=yymmn6. INFORMAT=yymmn6.
	from nfcs.sino_loan(keep = SORGCODE saccount dgetdate DDATEOPENED DDATECLOSED DBILLINGDATE iaccountstat where=(sorgcode like 'Q%' and datepart(DBILLINGDATE) < today() and iaccountstat in (1,2)))
;
quit;

proc sort in=sorgcodesaccount_ nodupkey;
by sorgcode saccount;
run;

proc sql;
	create table sino_loan_2 as select 
		Sorgcodesaccount_.SORGCODE
		,Sorgcodesaccount_.saccount
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
PROC SORT DATA=nfcs.SINO_LOAN(KEEP=iloanid sorgcode SACCOUNT ddateopened dbillingdate icreditlimit ibalance sname scerttype scertno)  OUT=rule_26_t;
BY SORGCODE SACCOUNT scertno;
RUN;

data rule_26_t2;
	set rule_26_t(WHERE=(SUBSTR(SORGCODE,1,1)='Q' AND SORGCODE^='Q10152900H0000' AND SORGCODE^='Q10152900H0001'));
	if sorgcode = lag(sorgcode) and SACCOUNT = lag(SACCOUNT) and ddateopened = lag(ddateopened) and scertno ^= lag(scertno);
	label
	sorgcode = ��������
	SACCOUNT = ҵ���
	sname = ����
	scerttype = ֤������
	scertno = ֤������
	ddateopened = ҵ��������
	dbillingdate = ����/Ӧ��������
	icreditlimit = ���Ŷ��
	ibalance = ���
	warning = ���
	;
run;
proc sql;
	create table rule_26 as select
	*
	from rule_26_t
	where iloanid in (select iloanid from rule_26_t2)
;
quit;
data rule_26;
	set rule_26(drop = iloanid where = (1=1 &timefilter.));
run;

/*Rule 27*/
/*ͬһ����ҵ��Ĳ�ͬ����ʹ�ò�ͬҵ��ŵ�����*/
/*����*/
PROC SORT DATA=nfcs.SINO_LOAN(KEEP= sorgcode sname scerttype scertno SACCOUNT ddateopened dbillingdate icreditlimit ibalance)  OUT=rule_27_t;
BY SORGCODE scertno SACCOUNT;
RUN;
data rule_27_t2;
	set rule_27_t;
	if sorgcode= lag(sorgcode) and scertno = lag(scertno) and SACCOUNT ^= lag(saccount) and ICREDITLIMIT = lag(ICREDITLIMIT) and ddateopened = lag(ddateopened);
	label
	sorgcode = ��������
	SACCOUNT = ҵ���
	sname = ����
	scerttype = ֤������
	scertno = ֤������
	ddateopened = ҵ��������
	dbillingdate = ����/Ӧ��������
	icreditlimit = ���Ŷ��
	ibalance = ���
	warning = ���
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
	set Rule_27(drop = warning where = (1=1 &timefilter.));
run;


/*Rule 28*/
/*ÿ�ڻ�����*����/���Ŷ�������ʱ���߼���ϵ�������⣨�껯��������Ӧ�ô���6%-60%�ĺ���Χ�ڣ�*/
/*����*/
proc sort data = nfcs.sino_loan(keep = sorgcode saccount dbillingdate SMONTHDURATION icreditlimit ITREATYPAYAMOUNT STREATYPAYDUE WHERE=(SUBSTR(sorgcode,1,1)='Q' AND sorgcode not in ('Q10152900H0000','Q10152900H0001'))) out = rule_28_t nodupkey;
by sorgcode saccount dbillingdate;
run;
data rule_28_t;
	set rule_28_t(where = (1=1 &timefilter.));
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
    from nfcs.sino_loan(where = (1=1 &timefilter.)) as T1
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
	from nfcs.sino_loan as T1
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
	from nfcs.sino_loan(where = (1=1 &timefilter.)) as T1
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
	T1.sname label = '����'
	,T1.scerttype label = '֤������'
	,T1.scertno label = '֤������'
	,T1.spin
	,T2.dbirthday label = '��������'
	from nfcs.sino_person_certification as T1
	left join nfcs.sino_person as T2
	on T1.spin = T2.spin
	where mdy(1,1,1935) <= T2.dbirthday < mdy(1,1,2005)
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
	T1.sname label = '����'
	,T1.scerttype label = '֤������'
	,T1.scertno label = '֤������'
	,T2.dbirthday label = "��������"
	FROM nfcs.sino_person_certification(where = (SUBSTR(SORGCODE,1,1)='Q' AND SORGCODE not in ('Q10152900H0000' 'Q10152900H0001'))) AS T1
	left JOIN nfcs.sino_person AS T2
	ON T1.spin = T2.spin
	where length(SCERTNO)=18 and MDY(input(SUBSTR(SCERTNO,11,2),2.),input(SUBSTR(SCERTNO,13,2),2.),input(SUBSTR(SCERTNO,7,4),4.)) ^= DATEPART(dbirthday) and scerttype ='0' and DBIRTHDAY is not null
	order by scertno
;
QUIT;
data rule_32;
	set rule_32;
	if scertno = lag(scertno) then delete;
run;



/*������*/

/*proc sql;*/
/*	create table config as select*/
/*	%sysfunc(trim(distinct T1.sorgcode)) as sorgcode*/
/*	,put(T1.sorgcode,$short_cd.) as shortname label = "�������"*/
/*	,T2.person label = "ר��Ա"*/
/*	from nfcs.sino_msg(where = (SUBSTR(SORGCODE,1,1)='Q' AND SORGCODE not in ('Q10152900H0000' 'Q10152900H0001'))) as T1*/
/*	trim join soc as T2*/
/*	on T1.sorgcode = T2.sorgcode*/
/*	order by person*/
/*	;*/
/*quit;*/
/*proc sql noprint;*/
/*	select count(distinct sorgcode) into :socnumber*/
/*	from nfcs.sino_msg*/
/*	;*/
/*quit;*/
/*proc sql noprint;*/
/*	select count(distinct person) into :personnumber*/
/*	from config*/
/*	;*/
/*quit;*/
proc sql noprint;
	select count(distinct sorgcode) into :socnumber
	from config
	;
quit;
proc sql noprint;
	select count(distinct person) into :personnumber
	from config
	;
quit;

data _null_;
set config;
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
%put &shortname100.;
%put &person100.;

/*������������ļ���*/
%macro shortnamefile;
%do i = 1 %to &socnumber.;
%ChkFile("&outfile.%sysfunc(trim(&&shortname&i.))");
%end;
%mend;

/*ɸѡʱ���*/
%macro timefilter;
%do i = 1 %to 30;
	data rule_&i.;
		set rule_&i.(where = (dgetdate >= mdy(7,1,2013) and &firstday. > datepart(dbillingdate) >= &firstday_two.) );
/*		if  &firstday. > datepart(dbillingdate) >= &firstday_two.;*/
	run;
%end;
%mend;

/*������������������ݵĶ�Ӧ��ϵ��*/

/*���*/
%macro outfile;
%do i = 1 %to 32;
	%do j = 1 %to &socnumber.;
data work.&&sorgcode&j.;
	set rule_&i.(where = (sorgcode = "%sysfunc(trim(&&sorgcode&j.))"));
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
libname xls excel "&outfile.%sysfunc(trim(&&shortname&j.))/rule_&i..xlsx";
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

/*��������ƴ��*/
%macro zipfile;
data _null_;  
/*    rc =system("cd &file_path."); */
%do i=1 %to &socnumber.;
	rc = system("'C:/Program Files/WinRAR/winrar.exe' a -ad -icbk -ep -y &outfile.%sysfunc(trim(&&shortname&i.))_&currmonth..rar &outfile.%sysfunc(trim(&&shortname&i.))\");
%end;
stop;
%do i = 1 %to &socnumber.;
	rc =system("'C:/Program Files/WinRAR/winrar.exe' a -ad -icbk -ep -y &outfile.%sysfunc(trim(&&shortname&i.))_&currmonth..rar D:\�߼�У����\�����������ϵͳ������˵��-V1.1.docx");

%end;
stop;
%do j = 1 %to &socnumber.;
/*	rc =system("'C:/Program Files/WinRAR/winrar.exe' a -ad -icbk -ep -ieq &outfile.&&&person&&j.._&currmonth..rar &outfile.%sysfunc(trim(&&shortname&j.))_&currmonth..rar");*/
	rc =system("'C:/Program Files/WinRAR/winrar.exe' a -ad -icbk -ep -y -df &outfile.%sysfunc(trim(&&person&j.))_&currmonth..rar &outfile.%sysfunc(trim(&&shortname&j.))_&currmonth..rar");

%end;
/*return;*/
run;  
%mend;  

/*����*/
ods listing close;
ods results off;
ODS TRACE OFF;
proc printto log=_null_;
run;
%shortnamefile;
/*%timefilter;*/
%outfile;
%zipfile;
proc printto log= log;
run;
ods listing;
ods results on;
ODS TRACE ON;

