%include "E:/林佳宁/code/config.sas";
%let lib = %str(work);
/*%let orgfilter = %nrstr(and sorgcode in ('Q10151000H3000' 'Q10152900H0900'));*/
%let orgfilter = %nrstr(and sorgcode in ('Q10152900H9800' 'Q10151000H8800' 'Q10152900H1D00' 'Q10152900HT400' 'Q10153300HDW00' 'Q10152900HFJ00' 'Q10152900H2Z00' 'Q10152900H1W00' 'Q10152900H8500' 'Q10152900H0900' 'Q10151000H3000' 'Q10152900HN500' 'Q10152900HAZ00' 'Q10152900H1200' 'Q10152900H1W00' 'Q10153900H7T00' 'Q10152900HC000' 'Q10151000H0G00' 'Q10155800HZ200' 'Q10152900H9C00' 'Q10155800H2P00' 'Q10152900HAL00' 'Q10152900HN300' 'Q10155800H5400' 'Q10152900H3500' 'Q10155800HCV00'
'Q10155800HS000' 'Q10152900H1400' 'Q10151000H0Y00' 'Q10152900HD900' 'Q10155800H3200' 'Q10152900H0900' 'Q10152900HU700' 'Q10151000H2800' 'Q10152900H7C00' 'Q10155800H6800' 'Q10151000HV200'));
data _null_;
	if %sysfunc(length(&orgfilter.)) = 0 then orgfilter = " ";
run;
%let timefilter = %str(and dgetdate >= &firstday_three. and &firstday. > datepart(dbillingdate) >= &firstday_three.);
%let NoteAddr = %unquote(%str(E:\林佳宁\code\数据质量\数据质量检查系统输出结果说明-V1.7.docx));

/*建立本期文件夹*/
/*写成宏*/

/*将数据从NFCS库中抽取到数据仓库中*/
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
/*1、结清时，上报"结算应还款日期"是否取结清日（不一定是问题的情况：结清日与最后一次还款不在同一天）*/
/*怀疑*/
proc sql;
create table rule_1 as
select t.sorgcode  label = "机构代码",
		smsgfilename label = "报文名称",
       t.saccount  label = "业务号", 
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
       t.sTermsfreq     label = "还款频率",
       t.dbillingdate   label = "结算应还款日期",
       t.drecentpaydate label = "最近一次实际还款日期",
       t.ischeduledamount label = "本月应还款金额",
       t.iactualpayamount label = "本月实际还款金额",
       t.iaccountstat   label = "账户状态",
       t.sPaystat24month label = "二十四月还款状态"
  from &lib..sino_loan(where = (dbillingdate ^= drecentpaydate and iaccountstat not in (1,2) and dgetdate >= mdy(7,1,2013) &orgfilter.)) as t
;
quit;

/*Rule 2*/
/*2、贷款逾期，帐户状态却为正常*/
/*错误*/
proc sql;
create table rule_2 as 
select a.sorgcode     label = "机构代码",
		smsgfilename label = "报文名称",
       a.saccount  label = "业务号", 
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
       a.dbillingdate     label = "结算应还款日期",
/*       a.drecentpaydate   label = "最近一次还款日期",*/
/*       a.ischeduledamount label = "本月应还款金额",*/
/*       a.iactualpayamount label = "本月实际还款金额",*/
/*       a.ibalance        label =  "贷款余额",*/
       a.icurtermspastdue label = "当前逾期期数",
/*       a.itermspastdue    label = "累计逾期期数",*/
/*       a.imaxtermspastdue label = "最高逾期期数",*/
       a.iaccountstat     label = "账户状态",
       a.sPaystat24month  label = "二十四月还款状态"
  from  &lib..sino_loan(where = (icurtermspastdue > 0 and iaccountstat = 1 &timefilter. &orgfilter.)) as a
  order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 3*/
/*5、在非开户当月的情况下，最近一次实际还款日期大于等于结算应还款日期，且实际还款金额为0 */
/*（确认一下最近一次实际还款日期取值问题）(不适用：宽限期的报送)*/
/*怀疑*/
proc sql;
create table rule_3 as
select a.sorgcode label = "机构代码",
		smsgfilename label = "报文名称",
a.saccount label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "还款频率",
a.dbillingdate label = "结算应还款日期",
a.drecentpaydate label = "最近一次实际还款日期",
a.ischeduledamount label = "本月应还款金额",
a.iactualpayamount label = "本月实际还款金额", 
a.iaccountstat  label = "账户状态",
a.sPaystat24month label = "二十四月还款状态"
from &lib..sino_loan(where = (dbillingdate <= drecentpaydate and iactualpayamount = 0 and drecentpaydate ^= ddateopened and sPaystat24month ^= '///////////////////////*' &timefilter. &orgfilter.)) as a
  order by sorgcode,saccount,dbillingdate
;
 quit;

/*Rule 4*/
/*8、贷款未到期，本月应还款金额大于本月实际还款金额，但"账户状态"正常*/
/*错误*/
proc sql;
create table rule_4 as select
		t.sorgcode label = "机构代码",
		smsgfilename label = "报文名称",
		t.saccount         label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
       t.dbillingdate     label = "结算应还款日期",
       t.drecentpaydate   label = "最近一次实际还款日期",
       t.ischeduledamount label = "本月应还款金额",
       t.iactualpayamount  label = "本月实际还款金额", 
       t.iaccountstat     label = "账户状态",
       t.sPaystat24month  label = "二十四月还款状态"
  from &lib..sino_loan (where = (iaccountstat = 1 and ddateclosed ^= dbillingdate and ischeduledamount > iACTUALPAYAMOUNT &timefilter. &orgfilter.)) as t
    order by sorgcode,saccount,dbillingdate

;
quit;

/*Rule 5*/
/*9、T+1开户时，"结算应还款日期"和"最近一次还款日期"应该等于"开户日期" （非T+1不适用）（T+1开户月--若不需还款和没发生还款，开户日上报一次）*/
/*错误*/
/*需要更新*/
proc sql;
create table rule_5 as select
a.sorgcode label = "机构代码",
smsgfilename label = "报文名称",
a.saccount label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "还款频率",
a.dbillingdate label = "结算应还款日期" format = DATETIME20. informat = DATETIME20.,
a.drecentpaydate label = '最近一次还款日期' format = DATETIME20. informat = DATETIME20.,
a.ischeduledamount label = "本月应还款金额",
a.iactualpayamount label = "本月实际还款金额",
a.sPaystat24month label = '二十四月还款状态'
  from &lib..sino_loan (where = (sPaystat24month = '///////////////////////*' and (dbillingdate ^= ddateopened or drecentpaydate ^= ddateopened) &timefilter. &orgfilter.)) as a
     order by sorgcode,saccount,dbillingdate
;
quit; 

/*Rule 6*/
/*10、24月还款状态最后一位是N时，但"本月应还款金额"大于"本月实际还款金额"*/
/*怀疑*/
proc sql;
create table rule_6 as select
 a.sorgcode label = "机构代码",
 		smsgfilename label = "报文名称",
a.saccount label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "还款频率",
a.dbillingdate label = "结算应还款日期",
a.drecentpaydate label = "最近一次还款日期",
a.ischeduledamount label = "本月应还款金额",
a.iactualpayamount label = '实际还款金额',
a.sPaystat24month label = '二十四月还款状态'
  from &lib..sino_loan(where = (substr(sPaystat24month,24,1) = 'N' and ischeduledamount > iactualpayamount &timefilter. &orgfilter.)) as a
 order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 7*/
/*11、非按月还款，除开户月和结清月外，"结算应还款日期"都应该取每月最后一天*/
/*怀疑*/
proc sql;
create table rule_7 as select
a.sorgcode label = "机构代码",
smsgfilename label = "报文名称",
a.saccount label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "还款频率",
a.dbillingdate label = "结算应还款日期",
a.drecentpaydate label = "最近一次还款日期",
a.ischeduledamount label = "本月应还款金额",
a.iactualpayamount label = '实际还款金额',
a.iaccountstat   label = "账户状态",
a.sPaystat24month label = '二十四月还款状态'
  from &lib..sino_loan(where = (substr(sPaystat24month,1,23) ^= '///////////////////////' and sTermsfreq ^= '03' and iaccountstat ^= 3 and datepart(dbillingdate) ^= intnx('month',datepart(dbillingdate),0,'end') &timefilter. &orgfilter.)) as a
 order by sorgcode,saccount,dbillingdate
;
quit;

 
/*Rule 8*/
/*14、"本月实际还款金额"大于""本月应还款金额""并且未报送特殊交易段*/
/*怀疑*/
proc sql;
create table rule_8 as select
		a.sorgcode     label = "机构代码",
				smsgfilename label = "报文名称",
       a.saccount         label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq       label = "还款频率",
       a.dbillingdate     label = "结算应还款日期",
       a.drecentpaydate   label = "最近一次还款日期",
 	   a.ischeduledamount label = "本月应还款金额",
       a.iactualpayamount label = "本月实际还款金额",
       a.sPaystat24month  label = "二十四月还款状态"
  from &lib..sino_loan(where = (1=1 &timefilter. &orgfilter.)) as a
where iActualpayamount > ischeduledamount and dbillingdate < ddateclosed and intnx('month',datepart(dbillingdate),0,'b') ^= intnx('month',datepart(ddateclosed),0,'b')
   and substr(sPaystat24month,23,1) in ('*','#','/','N') and iloanid not in (select iloanid from nfcs.sino_loan_spec_trade where speculiartradetype in ('4','5','9'))
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 9*/
/*15、贷款结清时，实际应还款金额不应为0*/
/*错误*/
proc sql;
create table rule_9 as select
		a.sorgcode     label = "机构代码",
				smsgfilename label = "报文名称",
       a.saccount         label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq       label = "还款频率",
       a.dbillingdate     label = "结算应还款日期",
       a.drecentpaydate   label = "最近一次还款日期",
       a.ischeduledamount label = "本月应还款金额",
       a.iactualpayamount label = "本月实际还款金额",
       a.sPaystat24month  label = '二十四月还款状态'
  from &lib..sino_loan(where = (substr(sPaystat24month,24,1) = 'C' and iaccountstat = 3 and iactualpayamount = 0 &timefilter. &orgfilter.)) as a
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 10*/
/*16、在"本月应还款金额"为0，且"本月实际还款金额"大于0的情况下，24月还款状态不应为星号*/
/*错误*/
proc sql;
create table rule_10 as select
a.sorgcode label = "机构代码",
		smsgfilename label = "报文名称",
a.saccount label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "还款频率",
a.dbillingdate label = "结算应还款日期",
a.drecentpaydate label = "最近一次还款日期",
a.ischeduledamount label = "本月应还款金额",
a.iactualpayamount label = '实际还款金额',
a.sPaystat24month label = '二十四月还款状态'
  from &lib..sino_loan(where = (ischeduledamount = 0 and iactualpayamount > 0 and substr(sPaystat24month,24, 1) = '*' &timefilter. &orgfilter.)) as a
/* where a.ischeduledamount = 0 and a.iactualpayamount > 0 and substr(a.sPaystat24month,24, 1) = '*' &timefilter. &orgfilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 11*/
/*18、上月正常还款，当月逾期未还款时，当前逾期总额应该等于"本月应还款金额"与"本月实际还款金额"之差*/
/*错误*/
proc sql;
create table rule_11 as select
a.sorgcode     label = "机构代码",   
		smsgfilename label = "报文名称",
a.saccount         label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq       label = "还款频率",
a.dbillingdate     label = "结算应还款日期",
a.drecentpaydate   label = "最近一次还款日期",
a.ischeduledamount label = "本月应还款金额", 
a.iactualpayamount label = "本月实际还款金额",
a.iamountpastdue   label = '当前逾期总额',
a.sPaystat24month  label = '二十四月还款状态'  
from &lib..sino_loan(where = (substr(sPaystat24month, 23, 2) = 'N1'
 and (ischeduledamount - iactualpayamount > iamountpastdue + 1 or ischeduledamount - iactualpayamount < iamountpastdue - 1) &timefilter. &orgfilter.)) as a
/* where substr(a.sPaystat24month, 23, 2) = 'N1'*/
/* and (ischeduledamount - iactualpayamount ^= iamountpastdue + 1 and ischeduledamount - iactualpayamount ^= iamountpastdue - 1 and ischeduledamount - iactualpayamount ^= iamountpastdue) &timefilter. &orgfilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 12*/
/*19、按月还款除开户外,"本月应还款金额"不应该为0(特殊情况除外)。已将开户当月提前还款的情况排除掉*/
/*错误*/
proc sql;
create table rule_12 as select
a.sorgcode label = "机构代码",
		smsgfilename label = "报文名称",
a.saccount label = "业务号",
scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "还款频率",
datepart(a.dbillingdate) as dbillingdate label = "结算应还款日期"  format = yymmdd10. informat = yymmdd10.,
a.drecentpaydate label = "最近一次还款日期",
a.ischeduledamount label = "本月应还款金额",
a.iactualpayamount label = '实际还款金额',
a.sPaystat24month label = '二十四月还款状态'
  from &lib..sino_loan(where = (sPaystat24month ^= '///////////////////////*' and sTermsfreq = '03' and ischeduledamount=0 and intnx('month',datepart(dbillingdate),0,'end') ^= intnx('month',datepart(ddateopened),0,'end')
&timefilter. &orgfilter.)) as a
/* where a.sPaystat24month ^= '///////////////////////*' and a.sTermsfreq = '03' and a.ischeduledamount=0 &timefilter. &orgfilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 13*/
/*20、按月还款到期后，"结算应还款日期"不等于月底*/
/*错误*/
proc sql;
create table rule_13 as select
		a.sorgcode label = "机构代码",
				smsgfilename label = "报文名称",
       a.saccount label = "业务号",
	   	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq label = "还款频率",
       a.dbillingdate label = "结算应还款日期",
       a.drecentpaydate label = "最近一次还款日期",
       a.ischeduledamount label = "本月应还款金额",       
		a.iactualpayamount label = "本月实际还款金额",
       a.sPaystat24month label = '二十四月还款状态'
  from &lib..sino_loan(where = (sTermsfreq = '03' and datepart(dbillingdate) ^= intnx('month',datepart(dbillingdate),0,'end') and iaccountstat ^= 3 
	and intnx('month',datepart(dbillingdate),0,'end') > intnx('month',datepart(ddateclosed),0,'end') &timefilter. &orgfilter.)) as a
/* where a.sTermsfreq = '03' and datepart(a.dbillingdate) ^= intnx('month',datepart(a.dbillingdate),0,'end') and a.iaccountstat ^= 3 */
/*	and intnx('month',datepart(a.dbillingdate),0,'end') > intnx('month',datepart(a.ddateclosed),0,'end') &timefilter. &orgfilter.*/
    order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 14*/
/*22、到期后，"累计逾期期数"、"当前逾期期数"、"最高逾期期数"不应该继续累计*/
/*错误*/
/*筛选到期后未结清的业务，并按业务号、账期排序*/
proc sql;
create table rule_14 as select
a.sorgcode     label = "机构代码",
smsgfilename label = "报文名称",
a.saccount         label = "业务号",
scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq       label = "还款频率",
a.dbillingdate     label = "结算应还款日期",
a.drecentpaydate   label = "最近一次还款日期",  
a.ischeduledamount label = "本月应还款金额",
a.iactualpayamount label = "本月实际还款金额",
a.iamountpastdue   label = '当前逾期总额',
a.icurtermspastdue label = "当前逾期期数",
a.itermspastdue label = "累计逾期期数",
a.imaxtermspastdue label = "最高逾期期数",
a.iaccountstat   label = "账户状态",
a.sPaystat24month  label = '二十四月还款状态',
a.iloanid
  from &lib..sino_loan(where = (iaccountstat = 2 and intnx('month',datepart(ddateclosed),0,'end') <= intnx('month',datepart(dbillingdate),0,'end') &timefilter. &orgfilter.)) as a
   order by sorgcode,saccount,dbillingdate
;
quit;
/*根据账期排序，如当前逾期、累计逾期、最高逾期三项没有继续累加的，则删除*/
data rule_14;
	set rule_14;
	if sorgcode = lag(sorgcode) and saccount = lag(saccount) and dbillingdate > lag(dbillingdate) and icurtermspastdue <= lag(icurtermspastdue) 
and itermspastdue <= lag(itermspastdue) and imaxtermspastdue <= lag(imaxtermspastdue) then delete;
run;
/*统计剩余的业务数，合规业务应该只剩到期日期当月的账期，不合规业务的账期数大于1，记录不合规业务的iloanid*/
proc sql;
	create table rule_14_t as select
		iloanid
		,count(iloanid) as cnt
		from rule_14
		group by iloanid
		having calculated cnt > 1
	;
quit;
/*将不合规业务到期后的账期全部筛选出*/
proc sql;
	create table rule_14 as select
		a.*
		from rule_14 as a
		where a.iloanid in (select iloanid from rule_14_t)
	;
quit;
/*V1.0添加-如到期业务当月未报送，且未发生提前还款的贷款业务，在到期当月后报送的，由于无法判别到期后第一笔账期当前逾期、累计逾期、最高逾期三项是否正确，
根据审慎原则，将该业务到期后第一笔业务加上怀疑标签 待添加*/
proc sort data = rule_14;
	by iloanid dbillingdate;
run;
data rule_14;
	set rule_14(drop = iloanid);
	if first.dbillingdate then delete;
run;

/*Rule 15*/
/*23、24月还款状态最后为1时，31-60未归还本金应该为0*/
/*错误*/
proc sql;
create table rule_15 as select
a.sorgcode label = "机构代码",
		smsgfilename label = "报文名称",
a.saccount label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "还款频率",
a.dbillingdate label = "结算应还款日期",
a.drecentpaydate label = "最近一次还款日期",
a.iamountpastdue30 label = '逾期31到60天未归还本金',
a.sPaystat24month label = '二十四月还款状态'
  from &lib..sino_loan( where  = (substr(sPaystat24month,24,1) = '1' and iamountpastdue30 ^= 0 &timefilter. &orgfilter.)) as a
/* where substr(a.sPaystat24month,24,1) = '1' and a.iamountpastdue30 ^= 0 &timefilter. &orgfilter.*/
  order by sorgcode,saccount,dbillingdate
;
quit;

 /*Rule 16*/
/*26、结清时,"结算应还款日期"，不等于最近一次实际还款日期*/
/*怀疑*/
proc sql;
create table rule_16 as select
a.sorgcode label = "机构代码",
		smsgfilename label = "报文名称",
a.saccount label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "还款频率",
a.dbillingdate label = "结算应还款日期",
a.drecentpaydate label = "最近一次还款日期",
a.ischeduledamount label = "本月应还款金额",
a.iactualpayamount label = '实际还款金额',
a.sPaystat24month label = '二十四月还款状态'
  from &lib..sino_loan(where = (drecentpaydate ^= dbillingdate and iaccountstat = 3 &timefilter. &orgfilter.)) as a
   order by sorgcode,saccount,dbillingdate
;
quit;

 /*Rule 17*/
/*27、"本月实际还款金额"大于等于"本月应还款金额"时，24个月还款状态取值不准确*/
/*错误*/
proc sql;
create table rule_17 as select
	   a.sorgcode     label = "机构代码",
	   		smsgfilename label = "报文名称",
       a.saccount         label = "业务号",
	   	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq       label = "还款频率",
       a.dbillingdate     label = "结算应还款日期",
       a.drecentpaydate   label = "最近一次还款日期",
       a.ischeduledamount label = '本月应还款金额',
       a.iactualpayamount label = "本月实际还款金额",
       a.sPaystat24month  label = '二十四月还款状态'
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
/*29、按月还款账户，上个月逾期本月账户正常的情况下，"本月实际还款金额"应该大于"本月应还款金额"*/
/*错误*/
proc sql;
create table rule_18 as select
a.sorgcode label = "机构代码",
		smsgfilename label = "报文名称",
a.saccount label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "还款频率",
a.dbillingdate label = "结算应还款日期",
a.drecentpaydate label = "最近一次还款日期",
a.ischeduledamount label = "本月应还款金额",
a.iactualpayamount label ="本月实际还款金额", 
a.sPaystat24month label = "二十四月还款状态"
  from &lib..sino_loan(where = (substr(sPaystat24month,24,1)='N' and substr(sPaystat24month,23,1) not in ('*','#','/','N') and iactualpayamount<=ischeduledamount and dbillingdate <= ddateclosed &timefilter. &orgfilter.)) as a 
/*where substr(a.sPaystat24month,24,1)='N' and substr(a.sPaystat24month,23,1) not in ('*','#','/','N') and a.iactualpayamount<=a.ischeduledamount and a.dbillingdate <= a.ddateclosed &timefilter. &orgfilter.*/
 order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 19*/
/*33、按月还款到期之后,当前逾期总额不应该小于余额*/
/*错误*/
proc sql;
create table rule_19 as select
a.sorgcode label = "机构代码",
		smsgfilename label = "报文名称",
a.saccount label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "还款频率",
a.dbillingdate label = "结算应还款日期",
a.ibalance label = '余额',
a.iamountpastdue label = '当前逾期总额',
a.iamountpastdue30 label = '逾期31_60天未归还本金',
a.iamountpastdue60 label = '逾期61_90天未归还本金',
a.iamountpastdue90 label = '逾期91_80天未归还本金',
a.iamountpastdue180  label = '逾期180天未归还本金',
a.sPaystat24month label = '二十四月还款状态'
  from &lib..sino_loan(where = ( sTermsfreq = '03' and dbillingdate > ddateclosed and (iamountpastdue < ibalance) &timefilter. &orgfilter.)) as a
/* where a.sTermsfreq = '03' and a.dbillingdate > a.ddateclosed and (a.iamountpastdue < a.ibalance) &timefilter. &orgfilter.*/
  order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 20*/
/*34、"还款频率"为固定，"当前逾期期数"、"累计逾期期数"不应该大于还款月数*/
/*怀疑*/
proc sql;
create table rule_20 as select
sorgcode     label = "机构代码",  
		smsgfilename label = "报文名称",
saccount  label = "业务号", 
scertno label = '证件号码',
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
sTermsfreq   label = "还款频率",
dbillingdate     label = "结算应还款日期",  
drecentpaydate   label = "最近一次实际还款日期",    
smonthduration  label = '还款月数',  
icurtermspastdue label = "当前逾期期数",
itermspastdue    label = "累计逾期期数",  
imaxtermspastdue label = "最高逾期期数"
from &lib..sino_loan(where = (sTermsfreq not in ('07', '08', '99') and input(smonthduration,4.) < itermspastdue &timefilter. &orgfilter.))
/*where sTermsfreq not in ('07', '08', '99') and input(smonthduration,4.) < itermspastdue &timefilter. &orgfilter.*/
order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 21*/
/*37、担保方式为含自然人保证，未上报担保段*/
/*怀疑*/
proc sql;
create table rule_21 as select
c.sorgcode  label = "机构代码",
		smsgfilename label = "报文名称",
c.saccount      label = "业务号",
  scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
c.iguaranteeway label = "担保方式"
  from &lib..sino_loan(where = (1=1 &timefilter. &orgfilter.)) as c 
where c.iguaranteeway in (3, 5, 7) and c.iloanid not in (select iloanid from nfcs.sino_loan_guarantee)
order by sorgcode,saccount
;
quit;

/*Rule 22*/
/*39、非开户月，最近一次"实际还款日期"不应该晚于"结算应还款日期"*/
/*怀疑*/
proc sql;
create table rule_22 as select
a.sorgcode label = "机构代码",
		smsgfilename label = "报文名称",
a.saccount label = "业务号",
	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
a.sTermsfreq label = "还款频率",
a.dbillingdate label = "结算应还款日期",
a.drecentpaydate label = "最近一次还款日期",
a.sPaystat24month label = '二十四月还款状态'
from &lib..sino_loan(where = (dbillingdate>ddateopened and drecentpaydate>dbillingdate &timefilter. &orgfilter.)) as a
/*where a.dbillingdate>a.ddateopened and a.drecentpaydate>a.dbillingdate &timefilter. &orgfilter.*/
;
quit;

/*Rule 23*/
/*40、正常结清的贷款，"本月应还款金额"应该等于"本月实际还款金额"*/
/*错误*/
proc sql;
create table rule_23 as select
 	   a.sorgcode label = "机构代码",
	   		smsgfilename label = "报文名称",
       a.saccount label = "业务号",
	   	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq label = "还款频率",
       a.dbillingdate label = "结算应还款日期",
       a.ischeduledamount label = "本月应还款金额",
       a.iactualpayamount label = "本月实际还款金额",
	   a.iaccountstat   label = "账户状态",
       a.sPaystat24month "二十四个月还款状态"
  from &lib..sino_loan( where = (substr(sPaystat24month,23,1) in ('*','#','/','N') and iaccountstat = 3 and  ischeduledamount ^= iactualpayamount  and  ddateclosed = dbillingdate &timefilter. &orgfilter.)) as a
/* where substr(a.sPaystat24month,23,1) in ('*','#','/','N') and  a.ischeduledamount ^= a.iactualpayamount  and  a.ddateclosed = a.dbillingdate &timefilter. &orgfilter.*/
 order by sorgcode,saccount,dbillingdate
 ;
 quit;

 /*Rule 24*/
/*48、发生地点应该到地市级*/
/* 怀疑*/
 proc sql;
create table rule_24 as select
 t.sorgcode label = "机构代码",
 		smsgfilename label = "报文名称",
 t.saccount label = "业务号",
 datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
 dbillingdate label = "结算应还款日期",
 t.sareacode label = '发生地点'
  from &lib..sino_loan(where = (substr(sareacode, 3, 4) = '0000' &timefilter. &orgfilter.)) as t
/* where substr(sareacode, 3, 4) = '0000' &timefilter. &orgfilter.*/
 order by sorgcode,saccount
;
quit;

/*41、根据征信管理条例规定无客户书面同意的情况下年收入不能采集，年收入默认为空即可*/
/*NFCS没有对应规定*/

/*补充校验规则*/

/*Rule 25*/
/*及时性*/
/*未入库的贷款业务清单*/
/*错误*/
proc sql;
	create table dmonth as select
	distinct(intnx('month',datepart(DBILLINGDATE),0,'b')) as dmonth FORMAT=yymmn6. INFORMAT=yymmn6.
	from &lib..sino_loan(keep = sorgcode dgetdate DBILLINGDATE where = (1=1 &timefilter. &orgfilter.))
/*	where today() > calculated dmonth > mdy(7,1,2013)*/
;
quit;
/*需要维护 用proc expand代替笛卡尔积*/
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
		t1.sorgcode label='机构代码'
		,t1.saccount label='业务号'
		,t1.scertno label = '证件号码'
		,t1.omonth label='贷款业务开立月份'
		,t1.cmonth label='贷款业务终止月份'
		,t1.dmonth label='未入库账期'
/*		,t2.dmonth as dmonth1*/
/*		,(case when t2.dmonth is not null then 1 else 0 end) as status label='入库状态'*/
		from sino_loan_2 as t1
		left join sino_loan_1 as t2
		on t1.sorgcode=t2.sorgcode and t1.saccount=t2.saccount and t1.dmonth=t2.dmonth
		where t2.dmonth is null
;
quit;

/*Rule 26*/
/*不同借款人使用同一贷款业务号的问题*/
/*怀疑*/
PROC SORT DATA=&lib..SINO_LOAN(KEEP= smsgfilename iloanid sorgcode SACCOUNT dgetdate ddateopened dbillingdate icreditlimit ibalance sname scerttype scertno)  OUT=rule_26_t;
BY SORGCODE SACCOUNT scertno;
RUN;

data rule_26_t2;
	set rule_26_t(WHERE=(SUBSTR(SORGCODE,1,1)='Q' AND SORGCODE^='Q10152900H0000' AND SORGCODE^='Q10152900H0001'));
	if sorgcode = lag(sorgcode) and SACCOUNT = lag(SACCOUNT) and ddateopened = lag(ddateopened) and scertno ^= lag(scertno);
	label
	sorgcode = 机构代码
	smsgfilename = 报文名称
	SACCOUNT = 业务号
	sname = 姓名
	scerttype = 证件类型
	scertno = 证件号码
	ddateopened = 业务开立日期
	dbillingdate = 结算/应还款日期
	icreditlimit = 授信额度
	ibalance = 余额
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
	sorgcode = 机构代码
		smsgfilename = 报文名称
	dgetdate = 加载日期
	SACCOUNT = 业务号
	sname = 姓名
	scerttype = 证件类型
	scertno = 证件号码
	ddateopened = 业务开立日期
	dbillingdate = 结算/应还款日期
	icreditlimit = 授信额度
	ibalance = 余额
	;
run;

/*Rule 27*/
/*同一贷款业务的不同账期使用不同业务号的问题*/
/*怀疑*/
PROC SORT DATA=&lib..SINO_LOAN(KEEP= sorgcode smsgfilename sname scerttype scertno SACCOUNT ddateopened dgetdate dbillingdate icreditlimit ibalance)  OUT=rule_27_t;
BY SORGCODE scertno SACCOUNT;
RUN;
data rule_27_t2;
	set rule_27_t(where = (1=1 &timefilter. &orgfilter.));
	if sorgcode= lag(sorgcode) and scertno = lag(scertno) and SACCOUNT ^= lag(saccount) and ICREDITLIMIT = lag(ICREDITLIMIT) and ddateopened = lag(ddateopened);
	label
	sorgcode = 机构代码
	smsgfilename = 报文名称
	SACCOUNT = 业务号
	sname = 姓名
	scerttype = 证件类型
	scertno = 证件号码
	ddateopened = 业务开立日期
	dbillingdate = 结算/应还款日期
	icreditlimit = 授信额度
	ibalance = 余额
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
	sorgcode = 机构代码
	smsgfilename = 报文名称
	dgetdate = 加载日期
	SACCOUNT = 业务号
	sname = 姓名
	scerttype = 证件类型
	scertno = 证件号码
	ddateopened = 业务开立日期
	dbillingdate = 结算/应还款日期
	icreditlimit = 授信额度
	ibalance = 余额
	;
run;


/*Rule 28*/
/*每期还款金额*期数/授信额度与贷款时长逻辑关系存在问题（年化贷款利率应该处于6%-60%的合理范围内）*/
/*怀疑*/
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
	dgetdate = 加载日期
	saccount = 业务号
	interest_year_single = 年化收益率(百分比)
	STREATYPAYDUE_num = 协定还款期数_整型
	ITREATYPAYAMOUNT_num = 协定期还款额_整型
	ICREDITLIMIT = 授信额度
	SMONTHDURATION = 还款月数
	STREATYPAYDUE = 协定还款期数
	ITREATYPAYAMOUNT = 协定期还款额
	dbillingdate = 结算/应还款日期
	;
run;

data rule_28;
retain sorgcode saccount;
	set rule_28(drop = interest_year_single interest STREATYPAYDUE_num ITREATYPAYAMOUNT_num MONTHDURATION);
run;

/*Rule 29*/
/*贷款业务的“结算/应还款日期”不应晚于报文上传时间*/
/*怀疑*/
proc sql;
    create table rule_29_temp as select
    T1.sorgcode label = "机构代码"
    ,T1.saccount label = "业务号",
	datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
	datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
	datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.
    ,T1.dbillingdate  label = "结算应还款日期"
    ,T2.duploadtime label = "报文上传时间"
    from &lib..sino_loan(where = (1=1 &timefilter. &orgfilter.)) as T1
    left join nfcs.sino_msg as T2
    on T1.SMSGFILENAME = T2.SMSGFILENAME and T1.dbillingdate > T2.duploadtime and T2.duploadtime is not null
;
quit;

proc sql;
	create table rule_29 as select
	T1.sorgcode label = "机构代码"
	,T1.saccount label = "业务号",
	datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.
	,T1.dbillingdate  label = "结算应还款日期"
	,T2.duploadtime label = "报文上传时间"
	from &lib..sino_loan as T1
	left join nfcs.sino_msg as T2
	on T1.SMSGFILENAME = T2.SMSGFILENAME 
	where T2.duploadtime is not null and datepart(T1.dbillingdate) > datepart(duploadtime)
	order by sorgcode,saccount,dbillingdate
;
quit;

/*Rule 30*/
/*贷款开立日期应该在一个合理的范围内：晚于1990年，早于报文上传时间*/
/*错误*/
proc sql;
	create table rule_30 as select
	T1.sorgcode label = "机构代码"
	,T1.saccount label = "业务号",
	datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
	datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
	datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.
	,T1.dbillingdate  label = "结算应还款日期"
	,T2.duploadtime label = "报文加载时间"
	from &lib..sino_loan(where = (1=1 &timefilter. &orgfilter.)) as T1
	left join nfcs.sino_msg as T2
	on T1.SMSGFILENAME = T2.SMSGFILENAME 
	where T2.duploadtime is not null and datepart(T1.ddateopened) > datepart(duploadtime) or datepart(T1.ddateopened) < mdy(1,1,1990)
	order by sorgcode,saccount,dbillingdate
;
quit; 

/*Rule 31*/
/*个人基本信息中的，“出生日期”应该在合理的范围内，晚于1935年，早于2005年*/
/*怀疑*/
proc sql;
	create table rule_31 as select
	T1.sorgcode label = "机构代码"
	,T1.sname label = '姓名'
	,T1.scerttype label = '证件类型'
	,T1.scertno label = '证件号码'
	,T1.spin
	,T2.dbirthday label = '出生日期'
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
/*个人基本信息中的，“出生日期”应该和身份证号码出生日期信息保持一致*/
/*怀疑*/
PROC SQL;
	CREATE TABLE rule_32 AS SELECT
	T1.sorgcode label = '机构代码'
	,T1.sname label = '姓名'
	,T1.scerttype label = '证件类型'
	,T1.scertno label = '证件号码'
	,T2.dbirthday label = "出生日期"
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
/*错误*/
校验24月还款状态的正确性
;

data rule_33_t;
	set &lib..sino_loan(keep = iid SPAYSTAT24MONTH );
	SPAYSTAT_flag = 0;
	array SPAYSTAT{*} $1. X1-X24;
	do i =1 to 24;
	SPAYSTAT{i} = substr(SPAYSTAT24MONTH,i,1);
	end;
/*24月还款状态出现跳位的情况*/
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
 	   a.sorgcode label = "机构代码",
	  	smsgfilename label = "报文名称",
       a.saccount label = "业务号",
	   	   scertno label = '证件号码' format = $18., 
datepart(dgetdate) as dgetdate label = "加载日期" format = yymmdd10.,
datepart(ddateopened) as ddateopened label = "开户日期" format = yymmdd10. informat = yymmdd10.,
datepart(ddateclosed) as ddateclosed label = "到期日期" format = yymmdd10. informat = yymmdd10.,
       a.sTermsfreq label = "还款频率",
       a.dbillingdate label = "结算应还款日期",
       a.ischeduledamount label = "本月应还款金额",
       a.iactualpayamount label = "本月实际还款金额",
	   a.iaccountstat   label = "账户状态",
       a.sPaystat24month "二十四个月还款状态"
	   from &lib..sino_loan(where = (1=1 &timefilter.)) as A
	   left join rule_33_t as B
	   on A.iid = B.iid
	   where B.SPAYSTAT_flag = 1
	;
quit;
	
/*结果输出*/

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

/*建立机构简称文件夹*/
%macro shortnamefile;
%do i = 1 %to &socnumber.;
%ChkFile("&outfile.%sysfunc(strip(&&shortname&i.))");
%end;
%mend;

/*筛选时间段*/
%macro timefilter;
%do i = 1 %to 33;
	data rule_&i.;
		set rule_&i.(where = (dgetdate >= mdy(7,1,2013) and &firstday. > datepart(dbillingdate) >= &firstday_two.) );
/*		if  &firstday. > datepart(dbillingdate) >= &firstday_two.;*/
	run;
%end;
%mend;

/*建立规则编号与规则内容的对应关系表*/

/*输出*/
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
/*x 'xcopy "D:\逻辑校验结果\testify_template.xlsx &outfile.%sysfunc(strip(&&shortname&j.))\rule_&i..xlsx /C /Y" & exit';*/
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

/*将机构简称打包*/
/*要注意WINRAR是否被占用*/
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


/*运行*/
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

