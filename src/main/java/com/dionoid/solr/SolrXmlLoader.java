package com.dionoid.solr;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.StrUtils;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.util.*;

public class SolrXmlLoader {

	protected static List<String> getDeleteIds(XMLStreamReader parser) throws XMLStreamException {
		List<String> ids = null; 
		StringBuilder text = new StringBuilder();
		boolean complete = false;
		while (!complete) {
			int event = parser.next();
			switch (event) {
				case XMLStreamConstants.START_ELEMENT:
					String mode = parser.getLocalName();
					if (!("id".equals(mode))) {
						String msg = "XML element <delete> has invalid XML child element: " + mode;
						//log.warn(msg);
						throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
					}
					text.setLength(0);
					break;
	
				case XMLStreamConstants.END_ELEMENT:
					String currTag = parser.getLocalName();
					if ("id".equals(currTag)) {
						if (ids == null) ids = new ArrayList<String>(); 
						ids.add(text.toString());
					} else if ("delete".equals(currTag)) {
						complete = true;
					} else {
						String msg = "XML element <delete> has invalid XML (closing) child element: " + currTag;
						//log.warn(msg);
						throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
					}
					break;
	
				// Add everything to the text
				case XMLStreamConstants.SPACE:
				case XMLStreamConstants.CDATA:
				case XMLStreamConstants.CHARACTERS:
					text.append(parser.getText());
					break;
			}
		}
		return ids;
	}

    protected static SolrInputDocument readDoc(XMLStreamReader parser) throws XMLStreamException {
        SolrInputDocument doc = new SolrInputDocument();

        String attrName = "";
        for (int i = 0; i < parser.getAttributeCount(); i++) {
            attrName = parser.getAttributeLocalName(i);
            if ("boost".equals(attrName)) {
                doc.setDocumentBoost(Float.parseFloat(parser.getAttributeValue(i)));
            } else {
            	System.err.println("XML element <doc> has invalid XML attr:" + attrName);
            }
        }

        StringBuilder text = new StringBuilder();
        String name = null;
        float boost = 1.0f;
        boolean isNull = false;
        String update = null;
        Collection<SolrInputDocument> subDocs = null;
        Map<String, Map<String, Object>> updateMap = null;
        boolean complete = false;
        while (!complete) {
            int event = parser.next();
            switch (event) {
                // Add everything to the text
                case XMLStreamConstants.SPACE:
                case XMLStreamConstants.CDATA:
                case XMLStreamConstants.CHARACTERS:
                    text.append(parser.getText());
                    break;

                case XMLStreamConstants.END_ELEMENT:
                    if ("doc".equals(parser.getLocalName())) {
                        if (subDocs != null && !subDocs.isEmpty()) {
                            doc.addChildDocuments(subDocs);
                            subDocs = null;
                        }
                        complete = true;
                        break;
                    } else if ("field".equals(parser.getLocalName())) {
                        // should I warn in some text has been found too
                        Object v = isNull ? null : text.toString();
                        if (update != null) {
                            if (updateMap == null) updateMap = new HashMap<>();
                            Map<String, Object> extendedValues = updateMap.get(name);
                            if (extendedValues == null) {
                                extendedValues = new HashMap<>(1);
                                updateMap.put(name, extendedValues);
                            }
                            Object val = extendedValues.get(update);
                            if (val == null) {
                                extendedValues.put(update, v);
                            } else {
                                // multiple val are present
                                if (val instanceof List) {
                                    @SuppressWarnings("unchecked")
									List<Object> list = (List<Object>) val;
                                    list.add(v);
                                } else {
                                    List<Object> values = new ArrayList<>();
                                    values.add(val);
                                    values.add(v);
                                    extendedValues.put(update, values);
                                }
                            }
                            break;
                        }
                        doc.addField(name, v, boost);
                        boost = 1.0f;
                        // field is over
                        name = null;
                    }
                    break;

                case XMLStreamConstants.START_ELEMENT:
                    text.setLength(0);
                    String localName = parser.getLocalName();
                    if ("doc".equals(localName)) {
                        if (subDocs == null)
                            subDocs = new ArrayList<SolrInputDocument>();
                        subDocs.add(readDoc(parser));
                    }
                    else {
                        if (!"field".equals(localName)) {
                            String msg = "XML element <doc> has invalid XML child element: " + localName;
                            System.err.println(msg);
                            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
                        }
                        boost = 1.0f;
                        update = null;
                        isNull = false;
                        String attrVal = "";
                        for (int i = 0; i < parser.getAttributeCount(); i++) {
                            attrName = parser.getAttributeLocalName(i);
                            attrVal = parser.getAttributeValue(i);
                            if ("name".equals(attrName)) {
                                name = attrVal;
                            } else if ("boost".equals(attrName)) {
                                boost = Float.parseFloat(attrVal);
                            } else if ("null".equals(attrName)) {
                                isNull = StrUtils.parseBoolean(attrVal);
                            } else if ("update".equals(attrName)) {
                                update = attrVal;
                            } else {
                            	System.err.println("XML element <field> has invalid XML attr: " + attrName);
                            }
                        }
                    }
                    break;
            }
        }

        if (updateMap != null)  {
            for (Map.Entry<String, Map<String, Object>> entry : updateMap.entrySet()) {
                name = entry.getKey();
                Map<String, Object> value = entry.getValue();
                doc.addField(name, value, 1.0f);
            }
        }

        return doc;
    }
}