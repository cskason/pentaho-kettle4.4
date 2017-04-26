package com.jk.di.trans.steps.dubboclient;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.userdefinedjavaclass.FieldHelper;
import org.pentaho.di.trans.steps.userdefinedjavaclass.TransformClassBase.Fields;

import com.hnair.opcnet.api.ods.psr.Passenger;
import com.hnair.opcnet.api.ods.psr.PassengerDetail;
import com.hnair.opcnet.api.ods.psr.PassengerExtraProduct;
import com.hnair.opcnet.api.ods.psr.PassengerResponse;
import com.hnair.opcnet.api.ods.psr.PassengerSM;
import com.hnair.opcnet.api.ods.psr.PassengerSS;

public class UserDefindProcess extends BaseStep implements StepInterface {

	public UserDefindProcess(StepMeta stepMeta,
			StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
		// TODO Auto-generated constructor stub
	}

	
	
	private List passengerList=null;

	@Override
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	{
	 
			
			
			Object[] r = getRow();
			 
			   if (r == null) {
			       setOutputDone();
			       return false;
			   }
			 
			   if (first){

			      first = false;

				// String fieldsNames = "id,ordinaryMark,icsPnr,psgNo,crsPnr,name,cnName,cardType,cardNum,carrier,carrierFlt,carrierFltSuf,fromPort,toPort,cabin,ticketNo,ffpMark,ffp,jinlukaMark,jinluka,ssMark,ss,mealMark,meal,groupMark,groupName,pnrNum,relation,dCreate,dOperate,tOperate,dcsHostNbr,dcsMark,dcsDelMark,preckiStatus,infantMark,status,statusInit,tCreate,mealCode,birthday,birthdayMark,age,olderMark,apNum,psrVUdgrade,psrUdgradeMark,psrUdgrade,psrValue,psrInbound,psrOutBound,psrGender,psrClass,psrSeatNo,psrBagTag,psrCkiAgent,psrCkiPid,psrCkiOffice,psrCkiTime,psrCkiType,psrBags,psrBagWht,FltDate,fltDateLocal,fltDateUtc,fltNo,accompanyNum,accompanyName,vipOrCip,vipNo,vipMark,psrInflt,psrInDate,psrInClass,psrInBrd,psrInSeat,psrInStn,psrOutFlt,psrOutDate,psrOutClass,psrOutStn,psrOutBrd,psrOutSeat,position,depAirportNo,arrAirportNo,psrEt,fltAlcdtw,psrEmdMark,psrSigMark,psrFttMark,psrGcgfMark,psrJinlukaType,vpProposedTitle,updateTime,sscCode,sscChnNote,meaCode,meaTitle,extraProductType,extraProductDesc,productCode,productName,description,amount";

			      RowMetaInterface inputRowMeta =  getInputRowMeta().clone();
			      
			      ValueMetaInterface vmi =  inputRowMeta.searchValueMeta("passenger");
			     int index = inputRowMeta.indexOfValue("passenger");
			     
			     if(index<-1)
			     {
			    	 throw new KettleException("找不到传入的参数:passenger");
			     }
			      
				 Object obj = r[index];
				 
				// PassengerResponse response = (PassengerResponse)obj;
				// passengerList  = (List)response.getPassenger();
				 //List fieldNameList = new ArrayList();
				 passengerList  = (List) obj;
				 RowMetaInterface outputRowMeta = new RowMeta();
				 
				 for(int i=0;i<passengerList.size();i++)
	               {
					 List valueList = new ArrayList();
	                  Passenger passenger=(Passenger)passengerList.get(i);
					PassengerDetail detail = passenger.getPassengerDetail();
					//List<PassengerSM> passageSm = passenger.getPassengerSM();
					//List<PassengerSS> passengerSS = passenger.getPassengerSS();
					//List<PassengerExtraProduct> passExtrProduct = passenger.getPassengerEP();

					Field[] fields = detail.getClass().getDeclaredFields();

					for (int j=0;j<fields.length;j++) {
	                     Field field =(Field)fields[j];
						if (!field.getType().getName().contains("java.util"))
						{
							if(first){
							int valueType = ValueMeta.getType(field.getType().getSimpleName());
							if(valueType==0){
								valueType=2;
							}
							ValueMetaInterface valueMeta = new ValueMeta(field.getName(),valueType);
							//valueMeta.setLength(-1);
							outputRowMeta.addValueMeta(valueMeta);
							
						}
							
							field.setAccessible(true); // 设置些属性是可以访问的
							Object val = null;
							try {
								val = field.get(detail);
							} catch (IllegalArgumentException e) {
								e.printStackTrace();
							} catch (IllegalAccessException e) {
								e.printStackTrace();
							} 
							valueList.add(val);
							
						}
					}
					first=false;
					 putRow(outputRowMeta, valueList.toArray()); 
				}
				 
				
				
			    }
			
			  
			return true;
	}
}
