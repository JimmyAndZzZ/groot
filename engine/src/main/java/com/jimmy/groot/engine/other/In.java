package com.jimmy.groot.engine.other;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorBoolean;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.jimmy.groot.engine.exception.EngineException;

import java.util.Collection;
import java.util.Map;

public class In extends AbstractFunction {

    @Override
    public AviatorObject call(final Map<String, Object> env, final AviatorObject str, final AviatorObject list) {
        Object javaObject = FunctionUtils.getJavaObject(str, env);
        Object value = FunctionUtils.getJavaObject(list, env);

        if (javaObject == null) {
            throw new EngineException("字段值为空");
        }

        if (!(value instanceof Collection)) {
            throw new EngineException("非集合数据");
        }

        Collection collection = (Collection) value;
        return AviatorBoolean.valueOf(collection.contains(javaObject));
    }

    @Override
    public String getName() {
        return "in";
    }
}
