package schemas;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Societe {
    private String permalink;
    private String name;
    private String homepage_url;
    private String category_list;
    private String status;
    private String country_code;
    private String state_code;
    private String region;
    private String city;
    private String founded_at;
}
