package com.rbkmoney.vortigon.resource.servlet

import com.rbkmoney.damsel.vortigon.VortigonServiceSrv
import com.rbkmoney.woody.thrift.impl.http.THServiceBuilder
import javax.servlet.GenericServlet
import javax.servlet.Servlet
import javax.servlet.ServletConfig
import javax.servlet.ServletRequest
import javax.servlet.ServletResponse
import javax.servlet.annotation.WebServlet

@WebServlet("/vortigon/v1")
class PartyShopServlet(
    private val vortigonServiceHandler: VortigonServiceSrv.Iface?,
) : GenericServlet() {

    private lateinit var thriftServlet: Servlet

    override fun init(config: ServletConfig) {
        super.init(config)
        thriftServlet = THServiceBuilder().build(VortigonServiceSrv.Iface::class.java, vortigonServiceHandler)
    }

    override fun service(req: ServletRequest, res: ServletResponse) {
        thriftServlet.service(req, res)
    }
}
