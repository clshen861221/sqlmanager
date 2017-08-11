@OneToOne(bean = "cn.com.zn.projectcheck.entity.T_yearplan")
private T_yearplan yearplan;
@Entity(primaryKey = "ypl_id")
public class T_yearplan

@OneToMany(bean = "cn.com.zn.projectcheck.entity.T_audit_solutoin")
private List<T_audit_solutoin> audit_solutoins = new ArrayList<T_audit_solutoin>();
@Entity(primaryKey = "ads_id")
public class T_audit_solutoin

1111