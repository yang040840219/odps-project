import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;

public class DownloadSample {
	private static String accessId = "LTAIzP30NiJ07JtK";
	private static String accessKey = "uPMGKoz3C67J7UGCGUik8GFZo4Fdom";
	private static String odpsUrl = "http://service.odps.aliyun.com/api";
	private static String project = "yunniao_dw";
	private static String table = "ods_beeper2_mongodb_driver_snapshots";

	public static void main(String args[]) throws Exception {
		String base = "/Users/yxl/data" ;
		String start = "2016-11-01";
		String end = "2016-11-30";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date first = sdf.parse(start);
		Date last = sdf.parse(end);
		List<String> listDates = new ArrayList<String>();
		while (first.compareTo(last) <= 0) {
			listDates.add(sdf.format(first));
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(first);
			calendar.add(Calendar.DAY_OF_MONTH, 1);
			first = calendar.getTime();
		}
		System.out.println(listDates);

		Account account = new AliyunAccount(accessId, accessKey);
		Odps odps = new Odps(account);
		odps.setEndpoint(odpsUrl);
		odps.setDefaultProject(project);
		TableTunnel tunnel = new TableTunnel(odps);

		for (String dstring : listDates) {
			System.out.println("start:" + dstring);
			PartitionSpec partitionSpec = new PartitionSpec("snapshot_date='"
					+ dstring + "'");
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(base + "/driver/" + dstring + ".txt")));
			try {
				DownloadSession downloadSession = tunnel.createDownloadSession(
						project, table, partitionSpec);
				System.out.println("Session Status is : "
						+ downloadSession.getStatus().toString());
				long count = downloadSession.getRecordCount();
				System.out.println("RecordCount is: " + count);
				RecordReader recordReader = downloadSession.openRecordReader(0,
						count);
				Record record;
				Pattern pattern = Pattern.compile("\t|\r|\n");

				while ((record = recordReader.read()) != null) {
					String line = consumeRecord(record, downloadSession.getSchema(), pattern);
					bw.write(line + '\n');
				}
				
				recordReader.close();
				bw.flush();
				bw.close();
			} catch (TunnelException e) {
				e.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}

	}

	private static String consumeRecord(Record record, TableSchema schema,
			Pattern pattern) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < schema.getColumns().size(); i++) {
			Column column = schema.getColumn(i);
			String colValue = null;
			switch (column.getType()) {
			case BIGINT: {
				Long v = record.getBigint(i);
				colValue = v == null ? null : v.toString();
				break;
			}
			case BOOLEAN: {
				Boolean v = record.getBoolean(i);
				colValue = v == null ? null : v.toString();
				break;
			}
			case DATETIME: {
				Date v = record.getDatetime(i);
				colValue = v == null ? null : v.toString();
				break;
			}
			case DOUBLE: {
				Double v = record.getDouble(i);
				colValue = v == null ? null : v.toString();
				break;
			}
			case STRING: {

				String v = record.getString(i);

				if (v == null) {
					colValue = null;
				} else {
					Matcher matcher = pattern.matcher(v.trim());
					colValue = matcher.replaceAll(" ");
				}
				break;
			}
			default:
				throw new RuntimeException("Unknown column type: "
						+ column.getType());
			}
			if (i != schema.getColumns().size()) {
				sb.append(colValue).append("|");
			}

		}
		String line = sb.toString();
		return line ;
	}
}