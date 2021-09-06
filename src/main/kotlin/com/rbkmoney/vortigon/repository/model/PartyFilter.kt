package com.rbkmoney.vortigon.repository.model

class PartyFilter {
    var email: String? = null
    var shopFilter: ShopFilter? = null
    var contractFilter: ContractFilter? = null
}

class ShopFilter {
    var locationUrl: String? = null
    var categoryName: String? = null
}

class ContractFilter {
    var legalAgreementSignedAt: String? = null
}
