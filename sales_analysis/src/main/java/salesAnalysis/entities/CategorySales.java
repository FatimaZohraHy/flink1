package salesAnalysis.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CategorySales {
    public String category;
    public Float totalSales;
    public Integer count;
}
