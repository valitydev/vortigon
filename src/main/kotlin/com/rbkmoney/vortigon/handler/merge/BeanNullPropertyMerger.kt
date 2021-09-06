package com.rbkmoney.vortigon.handler.merge

import com.rbkmoney.porter.listener.handler.merge.EventMerger
import org.springframework.beans.BeanUtils
import org.springframework.beans.BeanWrapper
import org.springframework.beans.BeanWrapperImpl
import org.springframework.stereotype.Component

@Component
class BeanNullPropertyMerger : EventMerger<Any> {

    override fun mergeEvent(source: Any, target: Any) {
        BeanUtils.copyProperties(source, target, *getNullPropertyNames(source))
    }

    protected fun getNullPropertyNames(source: Any): Array<String> {
        val src: BeanWrapper = BeanWrapperImpl(source)
        val pds = src.propertyDescriptors
        val emptyNames: MutableSet<String> = HashSet()
        for (pd in pds) {
            val srcValue = src.getPropertyValue(pd.name)
            if (srcValue == null) emptyNames.add(pd.name)
        }
        return emptyNames.toTypedArray()
    }
}
