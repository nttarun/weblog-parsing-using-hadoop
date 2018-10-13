package web.log.parse;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class ParseMapper extends Mapper<LongWritable,Text,NullWritable,Text>
{
	private static String log_pattern = "^(\\S+) (\\S+) (\\S+) \\[(.+?)\\] \"([^\"]*)\" (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\"";
	private static Pattern pattern = null;
	MultipleOutputs<NullWritable, Text> mos = null;
	private static int num_of_fields = 9;
	public void setup(Context context)
	{
		pattern = Pattern.compile(log_pattern);
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}
	public void cleanup(Context context) throws IOException,InterruptedException
	{
		mos.close();
	}
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		String formatedstring = value.toString().replaceAll("\t", " ");
		Matcher matcher = pattern.matcher(formatedstring);
		if(matcher.matches() && num_of_fields==matcher.groupCount())
		{
			String [] stringtokens = formatedstring.split(" ");
			String sepcat = getSeparateCatogory(stringtokens[5]);
			StringBuffer outputstring = new StringBuffer();
			outputstring.append(matcher.group(1)).append("\t");
			outputstring.append(matcher.group(2)).append("\t");
			outputstring.append(matcher.group(3)).append("\t");
			outputstring.append(matcher.group(4)).append("\t");
			outputstring.append(matcher.group(5)).append("\t");
			outputstring.append(sepcat).append("\t");
			outputstring.append(matcher.group(6)).append("\t");
			outputstring.append(matcher.group(7)).append("\t");
			outputstring.append(matcher.group(8)).append("\t");
			outputstring.append(matcher.group(9));
			mos.write("ParsedRecords", NullWritable.get(), new Text (outputstring.toString()));

		}
		else
		{
			mos.write("bad records",NullWritable.get(), value);
		}
		
	}
	public String getSeparateCatogory(String str)
	{
		String [] token = str.split(" ");
		String getsepcat = null;
		if(token.length==3)
		{
			getsepcat = processGetSepCat(token[1]);
		}
		else
		{
			getsepcat = processGetSepCat();
		}
		return getsepcat;
	}
	public String processGetSepCat(String str)
	{
		StringBuffer separateReqCategoryBuffer = new StringBuffer();
		
		String requestParamTokens [] = str.split("?");					
		String ParamString = "-";													
		boolean paramFlag = false;
		if (requestParamTokens.length == 2)											
		{
			paramFlag = true;
			ParamString = requestParamTokens[1];
		}
		else if (requestParamTokens.length > 2)										
		{
			paramFlag = true;
			StringBuffer paramStrBuff = new StringBuffer();
			for (int cnt = 1; cnt < requestParamTokens.length; cnt++)
			{
				paramStrBuff.append(requestParamTokens[cnt]);
				if (cnt < requestParamTokens.length - 1)
					paramStrBuff.append("?");
			}
			ParamString = paramStrBuff.toString();
		}
		String reqtokens [] = null;
		if(paramFlag)
		{
			reqtokens = requestParamTokens[0].split("/");
		}
		else
		{
			reqtokens = str.split("/");
		}
		
		int requestTokensLen = reqtokens.length;
		if (requestTokensLen == 0)												
		{
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append("-");
		}
	
		else if (requestTokensLen == 2)										
		{
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append("-").append("\t");

			separateReqCategoryBuffer.append(reqtokens[1]);
		}
		else if (requestTokensLen == 3)									
		{
			separateReqCategoryBuffer.append(reqtokens[1]).append("\t");
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append(reqtokens[2]);
		}
		else if (requestTokensLen == 4)										
		{
			separateReqCategoryBuffer.append(reqtokens[1]).append("\t");
			separateReqCategoryBuffer.append(reqtokens[2]).append("\t");
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append(reqtokens[3]);
		}
		else if (requestTokensLen == 5)
		{
			separateReqCategoryBuffer.append(reqtokens[1]).append("\t");
			separateReqCategoryBuffer.append(reqtokens[2]).append("\t");
			separateReqCategoryBuffer.append(reqtokens[3]).append("\t");
			separateReqCategoryBuffer.append("-").append("\t");
			separateReqCategoryBuffer.append(reqtokens[4]);
		}
		else if (requestTokensLen == 6)
		{
			separateReqCategoryBuffer.append(reqtokens[1]).append("\t");
			separateReqCategoryBuffer.append(reqtokens[2]).append("\t");
			separateReqCategoryBuffer.append(reqtokens[3]).append("\t");
			separateReqCategoryBuffer.append(reqtokens[4]).append("\t");
			separateReqCategoryBuffer.append(reqtokens[5]);
		}
		else if (requestTokensLen > 6)
		{
			separateReqCategoryBuffer.append(reqtokens[1]).append("\t");
			separateReqCategoryBuffer.append(reqtokens[2]).append("\t");
			separateReqCategoryBuffer.append(reqtokens[3]).append("\t");
			StringBuffer requestTokensBuffer = new StringBuffer();
			for (int cnt = 4; cnt < requestTokensLen - 1; cnt++)
			{
				requestTokensBuffer.append(reqtokens[cnt]).append("/");
			}
			separateReqCategoryBuffer.append(requestTokensBuffer).append("\t");
			separateReqCategoryBuffer.append(reqtokens[requestTokensLen - 1]);
		}
		
		
		separateReqCategoryBuffer.append("\t").append(ParamString);
		return separateReqCategoryBuffer.toString();
	}
	
	String processGetSepCat()
	{
		StringBuffer separateReqCategoryBuffer = new StringBuffer();
		
		separateReqCategoryBuffer.append("-").append("\t");				
		separateReqCategoryBuffer.append("-").append("\t");				
		separateReqCategoryBuffer.append("-").append("\t");				
		separateReqCategoryBuffer.append("-").append("\t");				
		separateReqCategoryBuffer.append("-").append("\t");				
		
		separateReqCategoryBuffer.append("-");							

		return separateReqCategoryBuffer.toString();
	}


		
	
	
}
