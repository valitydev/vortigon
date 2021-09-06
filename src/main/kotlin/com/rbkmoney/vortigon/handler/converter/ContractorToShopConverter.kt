package com.rbkmoney.vortigon.handler.converter

import com.rbkmoney.vortigon.domain.db.tables.pojos.Contractor
import com.rbkmoney.vortigon.domain.db.tables.pojos.Shop
import org.springframework.stereotype.Component

@Component
class ContractorToShopConverter : DomainConverter<Contractor, Shop> {
    override fun convert(contractor: Contractor): Shop {
        return Shop().apply {
            contractorType = contractor.contractorType
            regUserEmail = contractor.regUserEmail
            legalEntityType = contractor.legalEntityType
            russianLegalEntityName = contractor.russianLegalEntityName
            russianLegalEntityRegisteredNumber = contractor.russianLegalEntityRegisteredNumber
            russianLegalEntityInn = contractor.russianLegalEntityInn
            russianLegalEntityActualAddress = contractor.russianLegalEntityActualAddress
            russianLegalEntityPostAddress = contractor.russianLegalEntityPostAddress
            russianLegalEntityRepresentativePosition = contractor.russianLegalEntityRepresentativePosition
            russianLegalEntityRepresentativeFullName = contractor.russianLegalEntityRepresentativeFullName
            russianLegalEntityRepresentativeDocument = contractor.russianLegalEntityRepresentativeDocument
            russianLegalEntityBankAccount = contractor.russianLegalEntityBankAccount
            russianLegalEntityBankName = contractor.russianLegalEntityBankName
            russianLegalEntityBankPostAccount = contractor.russianLegalEntityBankPostAccount
            russianLegalEntityBankBik = contractor.russianLegalEntityBankBik
            internationalLegalEntityName = contractor.internationalLegalEntityName
            internationalLegalEntityTradingName = contractor.internationalLegalEntityTradingName
            internationalLegalEntityRegisteredAddress = contractor.internationalLegalEntityRegisteredAddress
            internationalActualAddress = contractor.internationalActualAddress
            internationalLegalEntityRegisteredNumber = contractor.internationalLegalEntityRegisteredNumber
            internationalLegalEntityCountryCode = contractor.internationalLegalEntityCountryCode
            privateEntityType = contractor.privateEntityType
            russianPrivateEntityEmail = contractor.russianPrivateEntityEmail
            russianPrivateEntityPhoneNumber = contractor.russianPrivateEntityPhoneNumber
            russianPrivateEntityFirstName = contractor.russianPrivateEntityFirstName
            russianPrivateEntitySecondName = contractor.russianPrivateEntitySecondName
            russianPrivateEntityMiddleName = contractor.russianPrivateEntityMiddleName
        }
    }
}
