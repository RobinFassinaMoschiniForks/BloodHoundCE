// Copyright 2025 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0
import { Button } from '@bloodhoundenterprise/doodleui';
import { AppLink, getTagUrlValue, useHighestPrivilegeTagId } from 'bh-shared-ui';
import { FC } from 'react';
import { useParams } from 'react-router-dom';

const InfoHeader: FC = () => {
    const { tagId: topTagId } = useHighestPrivilegeTagId();
    const { tierId = topTagId?.toString(), labelId } = useParams();
    const tagId = labelId === undefined ? tierId : labelId;

    return (
        <div className='flex justify-around basis-2/3'>
            <div className='flex justify-start gap-4 items-center basis-2/3'>
                <div className='flex items-center align-middle'>
                    <Button variant='primary' disabled={!tagId} asChild>
                        <AppLink
                            data-testid='zone-management_create-selector-link'
                            to={`/zone-management/save/${getTagUrlValue(labelId)}/${tagId}/selector`}>
                            Create Selector
                        </AppLink>
                    </Button>
                </div>
            </div>
            <div className='flex justify-start basis-1/3'>
                <input type='text' placeholder='search' className='hidden' />
            </div>
        </div>
    );
};

export default InfoHeader;
