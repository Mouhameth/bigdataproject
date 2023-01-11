package schemas;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class Tour {
    private String lien_societe;
    private String lien_tour_investissement;
    private String type_tour_investissement;
    private String code_tour_investissement;
    private String investi_en;
    private String montant_investi_en_euro;
}
