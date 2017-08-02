package com.jk.di.trans.steps.dubboclient;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.utils.ReferenceConfigCache;
import com.google.gson.Gson;
import com.jk.di.ui.trans.steps.dubboclient.utils.DynamicClassLoader;
import com.jk.di.ui.trans.steps.dubboclient.utils.ReflectionUtil;

public class DubboClient extends BaseStep implements StepInterface {
	
	private static Class<?> PKG = DubboClientMeta.class; // for i18n purposes, needed by Translator2!!
	
	private DubboClientMeta meta;
	private DubboClientData data;
	
	
	ReferenceConfigCache cache = ReferenceConfigCache.getCache();  //缓存，避免内存泄漏
	
	private static Gson gson = new Gson();

	public DubboClient(StepMeta stepMeta, StepDataInterface stepDataInterface,
			int copyNr, TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}
	
	/**
	 * 设置参数
	 * @param param
	 * @param value
	 * @return
	 */
	private String setParam(RowMetaAndData param,String value )
	{
		if(value==null || "".equals(value))
		{
			return "";
		}
		RowMetaInterface rowMeta = param.getRowMeta();
		Object [] data = param.getData();
		List<ValueMetaInterface> list = rowMeta.getValueMetaList();
		for(int i=0;i<list.size();i++)
		{
			ValueMetaInterface vm = list.get(i);
			String name = vm.getName();
			int index = value.indexOf("${"+name+"}");
			if(index>-1)
			{
				Object paramObjct = data[i];
				String paramValue="";
				if(paramObjct!=null)
				{
					paramValue=paramObjct.toString();
				}
				value = value.replace("${"+name+"}", paramValue);
			}
		}
		
		return value;
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
			throws KettleException {
		meta = (DubboClientMeta) smi;
		data = (DubboClientData) sdi;
		

		Object [] rowData = getRow();
		
		RowMetaAndData inputParam =null;
		if(rowData!=null)
		{
			data.inputRowMeta = getInputRowMeta().clone();
			inputParam = new RowMetaAndData(data.inputRowMeta, rowData);
		}
		
		
		data.outputRowMeta = new RowMeta();
		//meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);
		meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
		String appName = meta.getAppName();
		String registryAddr = meta.getRegistryAddr();
		String regProtocol = meta.getRegProtocol();
		String execInterface = meta.getExecInterface();
		String execMethodName = meta.getExecMethodName();
		Class<?>[] execMethodArgTypes = meta.getExecMethodArgTypes();
		Class<?> execMethodReturnType = meta.getExecMethodReturnType();
		
		String[] argValues = meta.getArgValues();
		
		
		if (this.first) {
		      EnvUtil.environmentInit();
		     // firstRun(getInputRowMeta(),execMethodReturnType);
		      this.first = false;
		    }
		
		// 检查参数
		if(Const.isEmpty(appName)) {
			throw new KettleException(BaseMessages.getString(PKG, "DubboClient.Error.AppNameEmpty"));
		}
		if(Const.isEmpty(registryAddr)) {
			throw new KettleException(BaseMessages.getString(PKG, "DubboClient.Error.RegistryAddrEmpty"));
		}
		if(Const.isEmpty(regProtocol)) {
			throw new KettleException(BaseMessages.getString(PKG, "DubboClient.Error.RegProtocolEmpty"));
		}
		if(Const.isEmpty(execInterface)) {
			throw new KettleException(BaseMessages.getString(PKG, "DubboClient.Error.ExecInterfaceEmpty"));
		}
		if(Const.isEmpty(execMethodName)) {
			throw new KettleException(BaseMessages.getString(PKG, "DubboClient.Error.ExecMethodEmpty"));
		}
		// ....
		
		// 使用dubbo client api 请求dubbo服务
		DynamicClassLoader dynaClassLoader = DubboClientMeta.dynaClassLoader;
		Class<?> serviceClazz = null;
		ReferenceConfig reference=null;
		
		try {
			serviceClazz = dynaClassLoader.loadClass(execInterface);
			// 配置当前应用
		   //设定dubbo日志级别
			com.alibaba.dubbo.common.logger.LoggerFactory.setLevel(com.alibaba.dubbo.common.logger.Level.ERROR);
			ApplicationConfig app = new ApplicationConfig();
			app.setName(appName);
			// 连接注册中心配置
			RegistryConfig registry = new RegistryConfig();
			registry.setProtocol(regProtocol); // 协议,一定要配置
			
			registryAddr= getTransMeta().environmentSubstitute(registryAddr); //获取变量地址
			registry.setAddress(registryAddr);

			// 注意：ReferenceConfig为重对象，内部封装了与注册中心的连接，以及与服务提供方的连接

			// 引用远程服务
			// 此实例很重，封装了与注册中心的连接以及与提供者的连接，需要缓存，否则可能造成内存和连接泄漏
			 reference = new ReferenceConfig();
			reference.setApplication(app);
			reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
			reference.setInterface(serviceClazz);
			reference.setVersion("1.0.0");
			reference.setProtocol("dubbo");
			
			
			Object service =cache.get(reference); //修改为从缓存获取，避免内存泄露
			
			//Object service = reference.get();
			
			
			// 得到具体的method
			Method execMethod = meta.getExecMethod();
			if(execMethod == null) {
				execMethod = ReflectionUtil.getMethod(service.getClass(), execMethodName, execMethodArgTypes);
				meta.setExecMethod(execMethod);
			}
			
			Object[] realArgValues = null;
			
			// TODO 参数类型变换
			// 对于复杂数据类型,传递过来的是json字符串,需要还原为实际的对象
			if(execMethodArgTypes != null) {
				realArgValues = new Object[execMethodArgTypes.length];
				for(int i = 0; i < execMethodArgTypes.length; i++) {
					Class<?> argType = execMethodArgTypes[i];
					String className = argType.getCanonicalName();
					if(!className.startsWith("java.lang")) {
						
						String value = argValues[i];
						if(inputParam!=null){
						    value = setParam( inputParam, value );
						}
						this.logBasic("paramType:"+argType.getName()+" paramValue:"+value);
						realArgValues[i] = gson.fromJson(value, argType);
					}
				}
			}
			
			// 通过反射来调用
			Object result = ReflectionUtil.invoke(execMethod, service, realArgValues);
			//reference.destroy(); // 是否是必须的?
			if(meta.getReturnGetSetMethod()!=null&& !"".equals(meta.getReturnGetSetMethod()))
			{
				Class resultClass = result.getClass();
				Method method = resultClass.getMethod(meta.getReturnGetSetMethod(), null);
				
				result = ReflectionUtil.invoke(method, result, null);
			}
			
			
			// 处理返回结果
			//processOutput(result, execMethodReturnType);
			processOutput(result);
			// 结束状态设置
			setOutputDone();
			
			
		} catch (Exception e) {
			  e.printStackTrace();//
			     reference.destroy(); //异常情况下，销毁
			     cache.destroy(reference);
			 
			throw new KettleException(e);
		}
		
		
		return false;
	}
	
	/**
	 * 把结果传递到下一个步骤
	 * @param result
	 * @throws IllegalAccessException 
	 * @throws IllegalArgumentException 
	 * @throws KettleStepException 
	 */
	private void processOutput(Object result) throws IllegalArgumentException, IllegalAccessException, KettleStepException
	{
		
		  // setOutPutRow( result);
		
		Object[] valueArray=null;
		
		String [] names = data.outputRowMeta.getFieldNames();
		if(result instanceof List)
	      {
			List<Object> resultList = (List) result;
			
			if(resultList.size()==0)
			{
				return;
			}
			
			Object resultObj=resultList.get(0);
			Class<?> clazz=resultObj.getClass();
			Field[] fields = clazz.getDeclaredFields();
			 valueArray = new Object[resultList.size()];
			List<Object> valueList = new ArrayList<Object>();
			
			//当类型为Map的时候
			if(resultObj instanceof Map){
			
			  for (int i=0;i<resultList.size();i++) {
				  
				  valueList = new ArrayList<Object>();
				   Map<String,Object> resultMap = (Map)resultList.get(i);
				   for(String key:names)
				   {
					   Object value = resultMap.get(key);
					   if(value==null)
					   {
						   value = "";
					   }
					   valueList.add(value); 
				   }
				   putRow(data.outputRowMeta, valueList.toArray());
				   valueArray[i]=valueList;
		       }
			}
			else //不为Map，为JavaBean的时候
			{
				if(names.length==0)
				{
				 List<String> fieldNameList = new ArrayList<String>();
				 for(Field field:fields)
				 {
					 if(!field.getType().getName().contains("java.util")){
					      fieldNameList.add(field.getName());
					 }
				 }
				 
				 names=fieldNameList.toArray(new String[fieldNameList.size()]); //转换成数组
				}
				
				 for (int i=0;i<resultList.size();i++) {
					  valueList = new ArrayList<Object>();
					  
					  
					  for(String fieldName:names)
					  {
						 try {
							Field field= resultList.get(i).getClass().getDeclaredField(fieldName);
							
							field.setAccessible( true ); // 设置些属性是可以访问的
					        Object val = field.get(resultList.get(i)); // 得到此属性的值   
					        valueList.add(val);
					        
						} catch (SecurityException e) {
							throw new KettleStepException("",e);
						} catch (NoSuchFieldException e) {
							
							throw new KettleStepException("没有该字段："+fieldName,e);
							
						}
						  
					  }
					  /**
					  for(Field field:fields)
						{
						  
							field.setAccessible( true ); // 设置些属性是可以访问的
					        Object val = field.get(resultList.get(i)); // 得到此属性的值   
					        valueList.add(val);
						}
						**/
					  putRow(data.outputRowMeta, valueList.toArray());
					  valueArray[i]=valueList;
				 }
				
				
			}
			
			
	      }
		else //类型不是List
		{
			List valueList = new ArrayList<Object>();
			Class<?> clazz=result.getClass();
			Field[] fields = clazz.getDeclaredFields();
			 valueArray = new Object[fields.length];
			for(int i=0;i<fields.length;i++)
			{
				
				Field  field = fields[i];
				
				int valueType = ValueMeta.getType(field.getType().getSimpleName());
				ValueMetaInterface valueMeta = new ValueMeta(field.getName(),valueType);
				valueMeta.setLength(-1);
				data.outputRowMeta.addValueMeta(valueMeta);
				
				
				
				field.setAccessible( true ); // 设置些属性是可以访问的
		        Object val = field.get(result); // 得到此属性的值   
		        valueArray[i]=val;
			}
			putRow(data.outputRowMeta, valueArray); 
	 }
		//putRow(data.outputRowMeta, valueArray);	
		
	}
	
	
	/**
	 * 设置输出列信息
	 * @param result
	 */
	private void setOutPutRow(Object result)
	{
		
		RowMetaInterface outRowMeta = new RowMeta();
		if(result==null)
		{
			return;
		}
		
		if(result instanceof List) //当结果的类型为List的时候
		{
			
			List<Object> resultList = (List<Object>) result;
			if(resultList.size()==0)
			{
				return;
			}
			Object obj=resultList.get(0);
			Class clazz=obj.getClass();
			
			if(obj instanceof Map)
			{
				Map<String,Object> objMap = (Map<String,Object>)obj;
				 for(String key:objMap.keySet())
				 {
					Object mapObj = objMap.get(key);
					int valueType = ValueMeta.getType(mapObj.getClass().getSimpleName());
					ValueMetaInterface valueMeta = new ValueMeta(key,valueType);
					valueMeta.setLength(-1);
					outRowMeta.addValueMeta(valueMeta);
					
				 }
				 	
			}
			else
			{
				Field[] fields = clazz.getDeclaredFields();
				for(Field field:fields)
				{
					if(!field.getType().getName().contains("java.util")){
					  int valueType = ValueMeta.getType(field.getType().getSimpleName());
					  ValueMetaInterface valueMeta = new ValueMeta(field.getName(),valueType);
					  valueMeta.setLength(-1);
					  outRowMeta.addValueMeta(valueMeta);
					}
				}
				
			}
			this.data.outputRowMeta = outRowMeta;
		}
		else
		{
			Class clazz = result.getClass();
			Field[] fields = clazz.getDeclaredFields();
			for(Field field:fields)
			{
				int valueType = ValueMeta.getType(field.getType().getSimpleName());
				ValueMetaInterface valueMeta = new ValueMeta(field.getName(),valueType);
				valueMeta.setLength(-1);
				outRowMeta.addValueMeta(valueMeta);
			}
			this.data.outputRowMeta = outRowMeta;
		}
		
	}
	
	
	@Override
	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		return super.init(smi, sdi);
	}

	@Override
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		super.dispose(smi, sdi);
	}
	
	
	private void firstRun(RowMetaInterface rowMeta,Class<?> methodReturnType)
	  {
		RowMetaInterface outRowMeta =  new RowMeta();
	    
	    Field[] fields = methodReturnType.getDeclaredFields();
	    
	    
	    ValueMetaInterface valueMeta = null;
	    for(Field field:fields)
	    {
	    	
	    	int valueType = ValueMeta.getType(field.getType().getSimpleName());
		      valueMeta = new ValueMeta(field.getName(),valueType);
		      valueMeta.setLength(-1); 
		      outRowMeta.addValueMeta(valueMeta);
	     }
	   
	   
	    this.data.outputRowMeta = outRowMeta;

	   
	  }


}
