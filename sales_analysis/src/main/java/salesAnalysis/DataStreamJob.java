package salesAnalysis;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import salesAnalysis.entities.OrderItem;
import salesAnalysis.entities.Product;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<OrderItem> orderItems = env.readCsvFile("C:\\Users\\ASUS\\Documents\\Master\\S2\\big_data_technology\\flink\\app\\flink_data_stream_job\\sales_analysis\\DataSet\\order_items.csv")
				.ignoreFirstLine()
				.pojoType(OrderItem.class, "OrderItemID", "OrderID", "ProductID", "Quantity", "PricePerUnit");

		DataSource<Product> products = env.readCsvFile("C:\\Users\\ASUS\\Documents\\Master\\S2\\big_data_technology\\flink\\app\\flink_data_stream_job\\sales_analysis\\DataSet\\products.csv")
				.ignoreFirstLine()
				.pojoType(Product.class,"productId","name","description","category","price");



		//joining of the data sets
		DataSet<Tuple6<String, String, Float, Integer, Float, String>> joined = orderItems
				.join(products)
				.where("ProductID")
				.equalTo("productId")
				.with((JoinFunction<OrderItem , Product,Tuple6<String, String, Float, Integer, Float, String>>)(first, second)
						-> new Tuple6<>(
						second.productId.toString(),
						second.name,
						first.PricePerUnit,
						first.Quantity,
						first.PricePerUnit * first.Quantity,
						second.category
				))
				.returns(TypeInformation.of(new TypeHint<Tuple6<String, String, Float, Integer, Float, String>>() {
				}));

		joined.print();


		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}
