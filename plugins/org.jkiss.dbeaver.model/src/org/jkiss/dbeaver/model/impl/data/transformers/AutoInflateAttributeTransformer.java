package org.jkiss.dbeaver.model.impl.data.transformers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.InflaterInputStream;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.data.DBDAttributeBinding;
import org.jkiss.dbeaver.model.data.DBDAttributeTransformer;
import org.jkiss.dbeaver.model.data.DBDDisplayFormat;
import org.jkiss.dbeaver.model.data.DBDValueHandler;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.impl.data.ProxyValueHandler;
import org.jkiss.dbeaver.model.impl.jdbc.data.JDBCContentBytes;
import org.jkiss.dbeaver.model.struct.DBSTypedObject;

public class AutoInflateAttributeTransformer implements DBDAttributeTransformer {

	@Override
	public void transformAttribute(DBCSession session, DBDAttributeBinding attribute, List<Object[]> rows, Map<String, Object> options) throws DBException {
		attribute.setPresentationAttribute(new TransformerPresentationAttribute(attribute, "COMPRESSED", -1, DBPDataKind.BINARY));
		attribute.setTransformHandler(new AutoInflateAttributeTransformer.AutoInflateHandler(attribute.getValueHandler()));
	}

	private static class AutoInflateHandler extends ProxyValueHandler {
		public AutoInflateHandler(DBDValueHandler target) {
			super(target);
		}

		@Override
		public String getValueDisplayString(DBSTypedObject column, Object value, DBDDisplayFormat format) {
			byte[] bytes = null;
			if (value instanceof byte[]) {
				bytes = (byte[]) value;
			} else if (value instanceof JDBCContentBytes) {
				bytes = ((JDBCContentBytes) value).getRawValue();
			}

			try {
				return bytes == null ? null : new String(new InflaterInputStream(new ByteArrayInputStream(bytes)).readAllBytes());
			} catch(IOException e) {
				return null;
			}
		}
	}
}
