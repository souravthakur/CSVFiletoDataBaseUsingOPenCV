import java.math.BigDecimal;

public enum DbDataTypeEnum {
	Decimal(BigDecimal.class),String(String.class),Boolean(Boolean.class),File(Byte.class);
	private Class<?> value;	
	DbDataTypeEnum(Class<?> value)
	{
		this.value=value;
	}
	public Class<?> getter()
	{
		return this.value;
	}
}
